package io.rtdi.bigdata.connector.pipeline.foundation.mapping;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.commons.jexl3.JexlExpression;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.rtdi.bigdata.connector.connectorframework.entity.SchemaMappingData.SchemaElement;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.IRecordMapping;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.IOUtils;

public class RecordMapping extends Mapping implements IRecordMapping {

	private Schema outputschema;
	private Map<String, Mapping> mapping = new HashMap<>();
	private String name;
	private SchemaHandler outputhandler;

	
	public RecordMapping(Schema outputschema) {
		super((JexlExpression) null);
		this.outputschema = outputschema;
	}

	public RecordMapping(File mappingfile, SchemaHandler outputhandler) throws PropertiesException {
		this(outputhandler.getValueSchema());
		this.outputhandler = outputhandler;
		outputhandler.setMapping(this);
		if (mappingfile != null) {
			try {
				ObjectMapper mapper = new ObjectMapper();
				SchemaElement mappingdata = mapper.readValue(mappingfile, SchemaElement.class);
				addRecordFields(this, mappingdata.getFields());
				this.name = getTargetSchemaname(mappingfile);
			} catch (IOException e) {
				throw new ConnectorRuntimeException("Reading the mapping file failed", e, null, mappingfile.getName());
			}			
		} else {
			throw new ConnectorRuntimeException("Mapping file cannot be null", null, "mappingfile parameter provided is null, internal error", outputhandler.getSchemaName().getName());
		}
	}

	public RecordMapping(String expression, Schema outputschema) throws IOException {
		super(expression);
		this.outputschema = outputschema;
	}
	
	public static String getTargetSchemaname(File mappingfile) {
		String mappingfilename = mappingfile.getName(); // e.g. "targetschema1_v34.json"
		Pattern p = Pattern.compile("_v[0-9]+\\.json$");
		Matcher matcher = p.matcher(mappingfilename);
		if (matcher.find()) {
			return mappingfilename.substring(0, matcher.start());
		} else {
			return null;
		}
	}
	
	public static void addRecordFields(RecordMapping rec, List<SchemaElement> elements) throws IOException {
		for (SchemaElement element : elements) {
			String expression = element.getExpression();
			if (expression != null) {
				String fieldname = element.getName();
				Field f = rec.outputschema.getField(fieldname);
				Schema baseschema = IOUtils.getBaseSchema(f.schema());
				Type t = baseschema.getType();
				switch (t) {
				case ARRAY:
					if (baseschema.getElementType().getType() == Type.RECORD) {
						ArrayMapping a = rec.addArrayMapping(fieldname, expression);
						RecordMapping arrayrecord = a.addRecordMapping();
						addRecordFields(arrayrecord, element.getFields());
					} else {
						rec.addPrimitiveMapping(fieldname, expression);
					}
					break;
				case RECORD:
					RecordMapping childrecord = rec.addRecordMapping(fieldname, expression);
					addRecordFields(childrecord, element.getFields());
					break;
				default:
					rec.addPrimitiveMapping(fieldname, expression);
				}
			}
		}
	}

	public void addPrimitiveMapping(String fieldname, String expression) throws IOException {
		Field f = outputschema.getField(fieldname);
		Schema baseschema = IOUtils.getBaseSchema(f.schema());
		Type t = baseschema.getType();
		if (t != Type.RECORD && t != Type.ARRAY) {
			mapping.put(fieldname, new PrimitiveMapping(expression));
		}
	}
	
	public RecordMapping addRecordMapping(String fieldname, String expression) throws IOException {
		Field f = outputschema.getField(fieldname);
		Schema baseschema = IOUtils.getBaseSchema(f.schema());
		Type t = baseschema.getType();
		if (t == Type.RECORD) {
			RecordMapping m = new RecordMapping(expression, baseschema);
			mapping.put(fieldname, m);
			return m;
		} else {
			return null; //TODO: error
		}
	}
	
	public ArrayMapping addArrayMapping(String fieldname, String expression) throws IOException {
		Field f = outputschema.getField(fieldname);
		Schema baseschema = IOUtils.getBaseSchema(f.schema());
		Type t = baseschema.getType();
		if (t == Type.ARRAY) {
			ArrayMapping m = new ArrayMapping(expression, baseschema.getElementType());
			mapping.put(fieldname, m);
			return m;
		} else {
			return null; //TODO: error
		}
	}

	public ArrayPrimitiveMapping addArrayPrimitiveMapping(String fieldname, String expressionarray, String expressionfield) throws IOException {
		Field f = outputschema.getField(fieldname);
		Schema baseschema = IOUtils.getBaseSchema(f.schema());
		Type t = baseschema.getType();
		if (t == Type.ARRAY) {
			ArrayPrimitiveMapping m = new ArrayPrimitiveMapping(expressionarray, baseschema.getElementType(), expressionfield);
			mapping.put(fieldname, m);
			return m;
		} else {
			return null; //TODO: error
		}
	}

	@Override
	public JexlRecord apply(JexlRecord input) throws IOException {
		JexlRecord out = new JexlRecord(outputschema);
		for (String field : mapping.keySet()) {
			Mapping m = mapping.get(field);
			Object o = m.evaluate(input);
			if ( o != null) {
				if (m instanceof PrimitiveMapping) {
					out.put(field, o);
				} else if (m instanceof RecordMapping) {
					if (o instanceof JexlRecord) {
						out.put(field, ((RecordMapping) m).apply((JexlRecord) o));
					} else if (o instanceof Number) {
						if (((Number) o).intValue() == 1) {
							out.put(field, ((RecordMapping) m).apply(input));
						}
					} else {
						// error
					}
				} else if (m instanceof ArrayPrimitiveMapping) {
					if (o instanceof List) {
						out.put(field, ((ArrayPrimitiveMapping) m).apply((List<?>) o, input));
					} else if (o instanceof Number) {
						int n = ((Number) o).intValue();
						out.put(field, ((ArrayPrimitiveMapping) m).apply(n, input));
					}
				} else {
					if (o instanceof List) {
						out.put(field, ((ArrayMapping) m).apply((List<?>) o));
					} else if (o instanceof Number) {
						int n = ((Number) o).intValue();
						out.put(field, ((ArrayMapping) m).apply(n, input));
					}
				}
			}
		}
		return out;
	}

	@Override
	public String getName() {
		return name;
	}
	
	@Override
	public Schema getOutputSchema() {
		return outputschema;
	}

	@Override
	public SchemaHandler getOutputSchemaHandler() {
		return outputhandler;
	}

}
