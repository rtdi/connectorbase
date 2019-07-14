package io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;

public class AvroRecordArray extends AvroRecordAbstract {

	public AvroRecordArray(String name, SchemaBuilder schema, String doc, boolean nullable, Object defaultValue, Order order) throws SchemaException {
		super(name, Schema.createArray(schema.getSchema()), doc, defaultValue, nullable, order, schema);
	}

	public AvroRecordArray(String name, SchemaBuilder schema, String doc, boolean nullable, Object defaultValue) throws SchemaException {
		super(name, Schema.createArray(schema.getSchema()), doc, nullable, defaultValue, schema);
	}

	public SchemaBuilder getArrayElementSchemaBuilder() {
		return schemabuilder;
	}
		
}
