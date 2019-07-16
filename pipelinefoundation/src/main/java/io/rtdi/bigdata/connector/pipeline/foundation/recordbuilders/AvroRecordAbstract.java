package io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;

public abstract class AvroRecordAbstract extends AvroField {

	protected SchemaBuilder schemabuilder;

	public AvroRecordAbstract(String name, Schema schema, String doc, boolean nullable, Object defaultValue, SchemaBuilder schemabuilder) {
		super(name, schema, doc, nullable, defaultValue);
		this.schemabuilder = schemabuilder;
	}

	public AvroRecordAbstract(String name, Schema schema, String doc, Object defaultValue, boolean nullable, Order order, SchemaBuilder schemabuilder) {
		super(name, schema, doc, defaultValue, nullable, order);
		this.schemabuilder = schemabuilder;
	}

	/**
	 * @param columnname name of the column to add
	 * @param schema Schema of the column to add
	 * @param description free form text
	 * @param nullable nullable == true?
	 * @return AvroField
	 * @throws SchemaException if the Schema is invalid
	 * 
	 * @see SchemaBuilder#add(String, Schema, String, boolean)
	 */
	public AvroField add(String columnname, Schema schema, String description, boolean nullable) throws SchemaException {
		return schemabuilder.add(columnname, schema, description, nullable);
	}
	
	/**
	 * @param columnname name of the column to add
	 * @param subschema schema of the record to add
	 * @param description free form text
	 * @param nullable nullable == true?
	 * @return AvroRecordField
	 * @throws SchemaException if the Schema is invalid
	 * 
	 * @see SchemaBuilder#addColumnRecord(String, SchemaBuilder, String, boolean)
	 */
	public AvroRecordField addColumnRecord(String columnname, SchemaBuilder subschema, String description, boolean nullable) throws SchemaException {
		return schemabuilder.addColumnRecord(columnname, subschema, description, nullable);
	}
	
	/**
	 * @param columnname name of the column to add
	 * @param description free form text
	 * @param nullable nullable == true?
	 * @param schemaname of the schema to be created; if null the column name is used
	 * @param schemadescription of the schema to be created; if null the column description is used
	 * @return AvroRecordField
	 * @throws SchemaException if the Schema is invalid
	 * 
	 * @see SchemaBuilder#addColumnRecord(String, String, boolean, String, String)
	 */
	public AvroRecordField addColumnRecord(String columnname, String description, boolean nullable, String schemaname, String schemadescription) throws SchemaException {
		return schemabuilder.addColumnRecord(columnname, description, nullable, schemaname, schemadescription);
	}
	
	/**
	 * @param columnname name of the column to add
	 * @param arrayelement Schema of type Avro array
	 * @param description free form text
	 * @param nullable nullable == true?
	 * @return AvroArray
	 * @throws SchemaException if the Schema is invalid
	 * 
	 * @see SchemaBuilder#addColumnArray(String, Schema, String, boolean)
	 */
	public AvroArray addColumnArray(String columnname, Schema arrayelement, String description, boolean nullable) throws SchemaException {
		return schemabuilder.addColumnArray(columnname, arrayelement, description, nullable);
	}
	
	/**
	 * @param columnname name of the column to add
	 * @param arrayelement SchemaBuilder for the array element
	 * @param description free form text
	 * @param nullable nullable == true?
	 * @return AvroRecordArray
	 * @throws SchemaException if the Schema is invalid
	 * 
	 * @see SchemaBuilder#addColumnRecordArray(String, SchemaBuilder, String, boolean)
	 */
	public AvroRecordArray addColumnRecordArray(String columnname, SchemaBuilder arrayelement, String description, boolean nullable) throws SchemaException {
		return schemabuilder.addColumnRecordArray(columnname, arrayelement, description, nullable);
	}
	
	/**
	 * @param columnname name of the column to add
	 * @param description free form text
	 * @param nullable nullable == true?
	 * @param schemaname of the schema to be created; if null the column name is used
	 * @param schemadescription of the schema to be created; if null the column description is used
	 * @return AvroRecordArray
	 * @throws SchemaException if the Schema is invalid
	 * 
	 * @see SchemaBuilder#addColumnRecordArray(String, String, boolean, String, String)
	 */
	public AvroRecordArray addColumnRecordArray(String columnname, String description, boolean nullable, String schemaname, String schemadescription) throws SchemaException {
		return schemabuilder.addColumnRecordArray(columnname, description, nullable, schemaname, schemadescription);
	}
	
}
