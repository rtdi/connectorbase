package io.rtdi.bigdata.connector.connectorframework.entity;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Parser;
import org.apache.avro.Schema.Type;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroAnyPrimitive;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroType;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.AvroArray;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.AvroField;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.IOUtils;

/**
 * This class serves two purposes, to render the UI properly and to save the content to a file.
 * So the sequence is that the SchemaMappingData get instantiated without a mapping, hence the structure is based on the target
 * schema and all expressions are empty.
 * The user defines mappings in the UI, posts the result as a partially filled out mapping and this SchemaMapping is saved as
 * mapping file, as a draft.
 * When the UI requests the SchemaMappingData the next time, it is again based on the most current target schema merged with the
 * mapping file content for the expressions.
 * 
 * Note that, although mapping and schema look similar, they are not. Recursions and element reuse is the difference. A schema might
 * contain recursions, might use the definition of Address for multiple fields like soldto/shipto/billto. In that case the schema
 * contains the Address fields just once. The mapping has to be different of course.
 *
 */
public class SchemaMappingData {
	private SchemaElement data;
	private String schemaname;

	public SchemaMappingData() {
		super();
	}
	
	/**
	 * Find a mapping from file - including drafts - based on the schema for the logical data model (targetschemaname) and the source schema name (remoteschemaname).
	 * 
	 * @param connector ConnectorController
	 * @param connectionname name of the connection
	 * @param remoteschemaname name of the source schema
	 * @param targetschemaname name of the target schema
	 * @return the matching file including draft versions, latest version, or null
	 */
	public static File getLastFilename(ConnectorController connector, String connectionname, String remoteschemaname, String targetschemaname) {
		File mappingdir = getMappingDir(connector, connectionname, remoteschemaname);
		if (mappingdir.isDirectory()) {
			FilenameFilter filter = new FilenameFilter() {
				private Pattern p = Pattern.compile("^" + targetschemaname + "_v[0-9]+(_draft)?\\.json");

				@Override
				public boolean accept(File dir, String filename) {
					return p.matcher(filename).matches();
				}
				
			};
			String[] mappingfiles = mappingdir.list(filter);
			if (mappingfiles != null && mappingfiles.length != 0) {
				Arrays.sort(mappingfiles);
				String lastfile = mappingfiles[mappingfiles.length-1];
				return new File(mappingdir.getAbsolutePath() + File.separatorChar + lastfile);
			}
		}
		return null;
	}

	/**
	 * Find a mapping from file based on the schema for the logical data model (targetschemaname) and the source schema name (remoteschemaname).
	 * 
	 * @param connector ConnectorController
	 * @param connectionname name of the connection
	 * @param remoteschemaname name of the source schema
	 * @return the matching file, latest version, or null
	 */
	public static File getLastActiveMapping(ConnectorController connector, String connectionname, String remoteschemaname) {
		File mappingdir = getMappingDir(connector, connectionname, remoteschemaname);
		if (mappingdir.isDirectory()) {
			FilenameFilter filter = new FilenameFilter() {
				private Pattern p = Pattern.compile("^.*_v[0-9]+\\.json");

				@Override
				public boolean accept(File dir, String filename) {
					return p.matcher(filename).matches();
				}
				
			};
			String[] mappingfiles = mappingdir.list(filter);
			if (mappingfiles != null && mappingfiles.length != 0) {
				Arrays.sort(mappingfiles);
				String lastfile = mappingfiles[mappingfiles.length-1];
				return new File(mappingdir.getAbsolutePath() + File.separatorChar + lastfile);
			}
		}
		return null;
	}

	public static File getMappingDir(ConnectorController connector, String connectionname, String remoteschemaname) {
		ConnectionController connection = connector.getConnection(connectionname);
		File dir = connection.getDirectory();
		String mappingdirpath = dir.getAbsolutePath() + File.separatorChar + "mappings" + File.separatorChar + remoteschemaname;
		return new File(mappingdirpath);
	}
		
	/**
	 * Used by the UI to merge the last mapping file with the latest version of the target schema.
	 * 
	 * @param connector ConnectorController
	 * @param connectionname name of the connection
	 * @param remoteschemaname name of the source schema
	 * @param targetschemaname name of the target schema
	 * @throws IOException if error
	 */
	public SchemaMappingData(ConnectorController connector, String connectionname, String remoteschemaname, String targetschemaname) throws IOException {
		SchemaHandler schemahandler = connector.getPipelineAPI().getSchema(targetschemaname);
		Schema schema = schemahandler.getValueSchema();
		this.schemaname = targetschemaname;
		SchemaElement mapping = null;
		File mappingfile = getLastFilename(connector, connectionname, remoteschemaname, targetschemaname);
		if (mappingfile != null) {
			ObjectMapper mapper = new ObjectMapper();
			mapping = mapper.readValue(mappingfile, SchemaElement.class);
		}
		
		data = new SchemaElement(targetschemaname, null, schema, mapping, new ArrayList<Schema>());
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
		private String expression;
		
		public SchemaElement() {
		}

		public SchemaElement(Field field, SchemaElement mapping, ArrayList<Schema> path) {
			this(field.name(), field.doc(), field.schema(), mapping, path);
		}

		public SchemaElement(String name, String description, Schema schema, SchemaElement mapping, ArrayList<Schema> path) {
			this.name = name;
			if (mapping != null) {
				this.expression = mapping.getExpression();
				this.description = mapping.getDescription();
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
				addRecordFields(mapping, path, s);
			} else if (s.getType() == Type.ARRAY) {
				Integer arraymin = AvroArray.getMin(s);
				max = AvroArray.getMax(s);
				if (arraymin != null && arraymin > min) {
					min = arraymin;
				}
				s = s.getElementType();
				datatype = AvroType.getAvroDatatype(s);
				int count = 0;
				if (expression != null) {
					try {
						count = Integer.valueOf(expression);
					} catch (NumberFormatException e) {
						count = 0;
					}
				}
				if (s.getType() == Type.RECORD) {
					if (count == 0) {
						addRecordFields(mapping, path, s);
					} else {
						for (int i=0; i<count; i++) {
							String n = "[" + String.valueOf(i) + "]";
							addField(new SchemaElement(n, null, s, mapping.getField(n), path));
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

		private void addRecordFields(SchemaElement mapping, ArrayList<Schema> path, Schema s) {
			datatype = Type.RECORD.name();
			for (Field field : s.getFields()) {
				if (!AvroField.isTechnical(field)) { // Hide technical fields that cannot be set anyhow
					if (Collections.frequency(path, field.schema()) < 1) {  // do not add recursive fields
						path.add(field.schema());
						SchemaElement c = null;
						if (mapping != null) {
							c = mapping.getField(field.name());
						}
						addField(new SchemaElement(field, c, path));
						path.remove(path.size()-1);
					}
				}
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
		
		public String toString() {
			return name + " (datatype: " + datatype + ")";
		}

		public List<SchemaElement> getFields() {
			return fields;
		}

		public String getName() {
			return name;
		}

		public String getDatatype() {
			return datatype;
		}

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
			if (fields != null) {
				for (SchemaElement f : fields) {
					nameindex.put(f.getName(), f);
				}
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

		public String getExpression() {
			return expression;
		}

		public void setExpression(String expression) {
			this.expression = expression;
		}
		
	}

	/**
	 * Save the mapping, the key schema used and the value schema. The latter is used in case the target schema does not exist yet.
	 * The save logic is to either 
	 * <ol><li>save as <I>name</I>_v1_draft in case this is brand new</li>
	 * <li>overwrite the last draft file in case the draft is the latest - continue editing a draft</li>
	 * <li>save as a new <I>name</I>_v2_draft in case the highest file is no draft and has version v1</li>
	 * </ol>
	 * 
	 * @param connector ConnectorController
	 * @param connectionname name of the connection
	 * @param remoteschemaname name of the source schema
	 * @param targetschemahandler SchemaHandler of the target schema
	 * @throws JsonGenerationException the json cannot be serialized
	 * @throws JsonMappingException the json write fails
	 * @throws IOException if error
	 */
	public void save(ConnectorController connector, String connectionname, String remoteschemaname, SchemaHandler targetschemahandler) 
			throws JsonGenerationException, JsonMappingException, IOException {
		String targetschemaname = targetschemahandler.getSchemaName().getName();
		File mappingfile = getLastFilename(connector, connectionname, remoteschemaname, targetschemaname);
		if (mappingfile == null) {
			File mappingdir = getMappingDir(connector, connectionname, remoteschemaname);
			mappingdir.mkdirs();
			mappingfile = new File(mappingdir.getAbsolutePath() + File.separator + targetschemaname + "_v1_draft.json");
		} else if (!mappingfile.getName().endsWith("_draft.json")) {
			// create a new version
			String filename = mappingfile.getName();
			int startpos = filename.lastIndexOf("_v") + 2;
			int endpos = filename.length()-5;
			String numberportion = filename.substring(startpos, endpos);
			int version = Integer.valueOf(numberportion) + 1;
			String mappingdir = mappingfile.getParent();
			mappingfile = new File(mappingdir + File.separator + targetschemaname + "_v" + String.valueOf(version) + "_draft.json");
		}
		ObjectMapper mapper = new ObjectMapper();
		mapper.writeValue(mappingfile, data);
		
		File valueschemafile = getSchemaFileForMapping(mappingfile);
		try (Writer w = new BufferedWriter(new FileWriter(valueschemafile));) {
			w.write(targetschemahandler.getValueSchema().toString(true));
		}
	}
	
	public static File getSchemaFileForMapping(File mappingfile) throws ConnectorRuntimeException {
		String schemafilepath = mappingfile.getAbsolutePath();
		String schemafilebasename = schemafilepath.substring(0, schemafilepath.length()-5);
		File valueschemafile = new File(schemafilebasename + ".avsc");
		return valueschemafile;
	}

	public static Schema getTargetSchema(File mappingfile) throws IOException {
		return new Parser().parse(getSchemaFileForMapping(mappingfile));
	}
}
