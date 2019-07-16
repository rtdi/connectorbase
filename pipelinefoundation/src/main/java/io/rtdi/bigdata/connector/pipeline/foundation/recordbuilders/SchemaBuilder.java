package io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;

import io.rtdi.bigdata.connector.pipeline.foundation.IOUtils;
import io.rtdi.bigdata.connector.pipeline.foundation.NameEncoder;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroInt;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;

/**
 * The base class for creating key and value schemas and to create subschemas 
 *
 */
public class SchemaBuilder {

	private List<Field> columns = new ArrayList<>();
	private Map<String, AvroField> columnnameindex = new HashMap<>();
	private Schema schema;
	private boolean isbuilt = false;
	private Map<String, SchemaBuilder> childbuilders = new HashMap<>();

	protected SchemaBuilder(String name, String namespace, String description) {
		schema = Schema.createRecord(name, description, namespace, false);
	}
	
	public SchemaBuilder(String name, String description) {
		this(name, null, description);
	}
	
	/**
	 * Add a column of datatype record based on a provided SchemaBuilder.<br>
	 * Useful if an Avro Schema has multiple fields with the same Record datatype.
	 * 
	 * @param columnname of the field to add
	 * @param subschema SchemaBuilder used to form the child record schema
	 * @param description optional 
	 * @param nullable is true if the column can be null
	 * @return AvroRecordField of the column added 
	 * @throws SchemaException if the subschema is null
	 */
	public AvroRecordField addColumnRecord(String columnname, SchemaBuilder subschema, String description, boolean nullable) throws SchemaException {
		validate(columnname);
		if (subschema == null) {
			throw new SchemaException("Schema cannot be null or empty");
		}
		AvroRecordField field = new AvroRecordField(columnname, subschema, description, nullable, JsonProperties.NULL_VALUE);
		add(field);
		childbuilders.put(columnname, subschema);
		return field;
	}
	
	/**
	 * Add a column of type record and create a new schema.
	 * 
	 * @param columnname of the field to add
	 * @param description optional
	 * @param nullable is true if the column can be null
	 * @param schemaname of the schema to be created; if null the column name is used
	 * @param schemadescription of the schema to be created; if null the column description is used
	 * @return AvroRecordField 
	 * @throws SchemaException if the schema is invalid
	 * 
	 * @see AvroRecordField#getSchemaBuilder()
	 */
	public AvroRecordField addColumnRecord(String columnname, String description, boolean nullable, String schemaname, String schemadescription) throws SchemaException {
		SchemaBuilder subschema = createNewSchema((schemaname != null?schemaname:columnname), (schemadescription != null?schemadescription:description));
		return addColumnRecord(columnname, subschema, description, nullable);
	}

	/**
	 * Add a column of datatype array-of-scalar.
	 * 
	 * @param columnname of the array column
	 * @param arrayelement One of the Avroxxxx schemas like {@link AvroInt#getSchema()}
	 * @param description explaining the use of the field
	 * @param nullable is true if optional
	 * @return AvroArray, the added column
	 * @throws SchemaException if the schema is invalid
	 */
	public AvroArray addColumnArray(String columnname, Schema arrayelement, String description, boolean nullable) throws SchemaException {
		validate(columnname);
		AvroArray field = new AvroArray(columnname, arrayelement, description, nullable, JsonProperties.NULL_VALUE);
		add(field);
		return field;
	}

	/**
	 * Add a column of datatype array-of-records based on an existing SchemaBuilder.
	 * 
	 * @param columnname of the array column
	 * @param arrayelement created via {@link ValueSchema#createNewSchema(String, String)}
	 * @param description explaining the use of the field
	 * @param nullable is true if optional
	 * @return AvroRecordArray, the added column
	 * @throws SchemaException if the schema is invalid
	 */
	protected AvroRecordArray addColumnRecordArray(String columnname, SchemaBuilder arrayelement, String description, boolean nullable) throws SchemaException {
		validate(columnname);
		AvroRecordArray field = new AvroRecordArray(columnname, arrayelement, description, nullable, JsonProperties.NULL_VALUE);
		add(field);
		childbuilders.put(columnname, arrayelement);
		return field;
	}

	/**
	 * @param columnname of the field to add
	 * @param description optional
	 * @param nullable is true if the column can be null
	 * @param schemaname of the schema to be created; if null the column name is used
	 * @param schemadescription of the schema to be created; if null the column description is used
	 * @return AvroRecordArray
	 * @throws SchemaException if the schema is invalid
	 */
	public AvroRecordArray addColumnRecordArray(String columnname, String description, boolean nullable, String schemaname, String schemadescription) throws SchemaException {
		SchemaBuilder subschema = createNewSchema((schemaname != null?schemaname:columnname), (schemadescription != null?schemadescription:description));
		return addColumnRecordArray(columnname, subschema, description, nullable);
	}

	/**
	 * Add columns to the current schema before it is built.<br>
	 * A typical call will look like 
	 * <pre>add("col1", AvroNVarchar.getSchema(10), "first col", false);</pre>
	 * 
	 * @param columnname of the field to add
	 * @param schema of the column; see io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes
	 * @param description of the column or null
	 * @param nullable is true if the column is optional
	 * @return AvroField to set other properties of the field (fluent syntax)
	 * @throws SchemaException if the schema is invalid
	 * 
	 * @see AvroField#AvroField(String, Schema, String, boolean, Object)
	 */
	public AvroField add(String columnname, Schema schema, String description, boolean nullable) throws SchemaException {
		validate(columnname, schema);
		AvroField field = new AvroField(columnname, schema, description, nullable, null); // JsonProperties.NULL_VALUE
		add(field);
		return field;
	}

	public AvroField add(Field f) throws SchemaException {
		validate(f.name(), schema);
		AvroField field = new AvroField(f);
		add(field);
		return field;
	}
	
	public AvroField getField(String columnname) {
		return columnnameindex.get(columnname);
	}

	private void add(AvroField field) throws SchemaException {
		columns.add(field);
		columnnameindex.put(field.name(), field);
	}
	
	private void validate(String columnname) throws SchemaException {
		if (columnname == null || columnname.length() == 0) {
			throw new SchemaException("Columnname cannot be null or empty");
		}
		if (isbuilt) {
			throw new SchemaException("Record had been built already, cannot add more columns");
		}
	}
		
	private void validate(String columnname, Schema schema) throws SchemaException {
		validate(columnname);
		if (schema == null) {
			throw new SchemaException("Schema cannot be null or empty");
		}
	}
	
	protected SchemaBuilder createNewSchema(String name, String schemadescription) throws SchemaException {
		if (name == null || name.length() == 0) {
			throw new SchemaException("Schema name cannot be null or empty");
		}
		SchemaBuilder b = new SchemaBuilder(name, schema.getNamespace(), schemadescription);
		return b;
	}

	public boolean contains(String columnname) {
		return columnnameindex.containsKey(columnname);
	}

	public Schema getSchema() throws SchemaException {
		return schema;
	}
	
	/**
	 * Once all columns are added to the schema it can be built and is locked then. 
	 * The build() process goes through all child record builders as well, building the entire schema.
	 * 
	 * @throws SchemaException if the schema has no columns
	 */
	public void build() throws SchemaException {
		if (columns.size() == 0) {
			throw new SchemaException("No schema definition found, schema builder has no columns");
		}
		if (!isbuilt) {
			isbuilt = true;
			schema.setFields(columns);
			for (SchemaBuilder child : childbuilders.values()) {
				child.build();
			}
		}
	}
	
	public String getName() {
		return schema.getName();
	}

	public String getDescription() {
		return schema.getDoc();
	}

	@Override
	public String toString() {
		return schema.getName();
	}
	
	public Schema getColumnSchema(String columnname) throws SchemaException {
		String encodedname = NameEncoder.encodeName(columnname);
		Field f = columnnameindex.get(encodedname);
		if (f != null) {
			return f.schema();
		} else {
			throw new SchemaException("Requested field \"" + columnname + "\" (encoded: \"" + encodedname + "\") does not exist");
		}
	}


	public static Schema getSchemaForArray(GenericRecord valuerecord, String fieldname) throws SchemaException {
		Field f = valuerecord.getSchema().getField(fieldname);
		if (f == null) {
			throw new SchemaException("The record and its supporting schema \"" + valuerecord.getSchema().getName() + 
					"\" does not have a field \"" + fieldname + "\"");
		}
		Schema s = IOUtils.getBaseSchema(f.schema());
		if (s.getType() != Type.ARRAY) {
			throw new SchemaException("The record and its supporting schema \"" + valuerecord.getSchema().getName() + 
					"\" has a field \"" + fieldname + "\" but this is no array");
		}
		return s.getElementType();
	}

	public static Schema getSchemaForNestedRecord(GenericRecord valuerecord, String fieldname) throws SchemaException {
		Field f = valuerecord.getSchema().getField(fieldname);
		if (f == null) {
			throw new SchemaException("The record and its supporting schema \"" + valuerecord.getSchema().getName() + 
					"\" does not have a field \"" + fieldname + "\"");
		}
		Schema s = IOUtils.getBaseSchema(f.schema());
		if (s.getType() != Type.RECORD) {
			throw new SchemaException("The record and its supporting schema \"" + valuerecord.getSchema().getName() + 
					"\" has a field \"" + fieldname + "\" but this is no nested record");
		}
		return s;
	}

}
