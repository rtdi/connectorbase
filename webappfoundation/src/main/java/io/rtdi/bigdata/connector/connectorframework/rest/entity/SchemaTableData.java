package io.rtdi.bigdata.connector.connectorframework.rest.entity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.rtdi.bigdata.connector.pipeline.foundation.IOUtils;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroAnyPrimitive;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroType;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.AvroArray;

public class SchemaTableData {
	private SchemaElement data;
	private String schemaname;

	public SchemaTableData() {
		super();
	}
		
	public SchemaTableData(
			IPipelineAPI<?, ?, ?, ?> api,
			String schemaname) throws PropertiesException {
		SchemaHandler schemahandler = api.getSchema(schemaname);
		Schema schema = schemahandler.getValueSchema();
		this.schemaname = schemaname;
		data = new SchemaElement(schemaname, null, schema, new ArrayList<Schema>());
	}

	public SchemaTableData(String remotename, Schema schema) {
		this.schemaname = remotename;
		data = new SchemaElement(remotename, null, schema, new ArrayList<Schema>());
	}

	public SchemaElement getData() {
		return data;
	}

	public void setData(SchemaElement data) {
		this.data = data;
	}
	
	@Override
	public String toString() {
		return super.toString();
	}

	
	public String getSchemaname() {
		return schemaname;
	}

	public void setSchemaname(String schemaname) {
		this.schemaname = schemaname;
	}


	public static class SchemaElement {

		private List<SchemaElement> fields = null;
		private String name = null;
		private String datatype;
		private String cardinality = null;
		private HashMap<String, SchemaElement> nameindex = null;
		private String description;
		
		public SchemaElement() {
		}

		public SchemaElement(Field field, ArrayList<Schema> path) {
			this(field.name(), field.doc(), field.schema(), path);
		}

		public SchemaElement(String name, String description, Schema schema, ArrayList<Schema> path) {
			this.name = name;
			this.description = description;
			if (this.description == null) {
				this.description = schema.getDoc();
			}
			int min;
			Integer max = 1;
			if (schema.getType() == Type.UNION && schema.getTypes().get(0).getType() == Type.NULL) {
				min = 0;
			} else {
				min = 1;
			}
			Schema s = IOUtils.getBaseSchema(schema);
			if (s.getType() == Type.UNION) {
				datatype = AvroAnyPrimitive.NAME;
			} else if (s.getType() == Type.RECORD) {
				datatype = Type.RECORD.name();
				for (Field field : s.getFields()) {
					if (Collections.frequency(path, field.schema()) < 1) {  // do not add recursive fields
						path.add(field.schema());
						addField(new SchemaElement(field, path));
						path.remove(path.size()-1);
					}
				}
			} else if (s.getType() == Type.ARRAY) {
				Integer arraymin = AvroArray.getMin(s);
				max = AvroArray.getMax(s);
				if (arraymin != null && arraymin > min) {
					min = arraymin;
				}
				s = s.getElementType();
				datatype = AvroType.getAvroDatatype(s);
				if (s.getType() == Type.RECORD) {
					for (Field field : s.getFields()) {
						if (Collections.frequency(path, field.schema()) < 1) {  // do not add recursive fields
							path.add(field.schema());
							addField(new SchemaElement(field, path));
							path.remove(path.size()-1);
						}
					}
				}
			} else {
				datatype = AvroType.getAvroDatatype(s);
			}
			if (max == null) {
				if (min == 0) {
					cardinality = "0..n";
				} else if ( min == 1) {
					cardinality = "1..n";
				} else {
					cardinality = String.valueOf(min) + "..n";
				}
			} else {
				cardinality = String.valueOf(min) + ".." + String.valueOf(max);
			}
		}
		
		private void addField(SchemaElement element) {
			if (fields == null) {
				fields = new ArrayList<>();
				nameindex = new HashMap<>();
			}
			fields.add(element);
			nameindex.put(element.getName(), element);
		}
		
		@Override
		public String toString() {
			return name + " (datatype: " + datatype + ")";
		}

		@XmlElement
		public List<SchemaElement> getFields() {
			return fields;
		}

		@XmlElement
		public String getName() {
			return name;
		}

		@XmlElement
		public String getDatatype() {
			return datatype;
		}

		@XmlTransient
		@JsonIgnore
		public SchemaElement getField(String fieldname) {
			if (nameindex != null) {
				return nameindex.get(fieldname);
			} else {
				return null;
			}
		}

		public void setFields(ArrayList<SchemaElement> fields) {
			this.fields = fields;
			nameindex = new HashMap<>();
			for (SchemaElement f : fields) {
				nameindex.put(f.getName(), f);
			}
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setDatatype(String datatype) {
			this.datatype = datatype;
		}

		public String getCardinality() {
			return cardinality;
		}

		public void setCardinality(String cardinality) {
			this.cardinality = cardinality;
		}

		public String getDescription() {
			return description;
		}

		public void setDescription(String description) {
			this.description = description;
		}
		
	}

}
