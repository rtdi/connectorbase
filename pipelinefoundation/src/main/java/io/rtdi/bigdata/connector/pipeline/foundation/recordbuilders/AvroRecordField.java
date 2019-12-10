package io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;

public class AvroRecordField extends AvroRecordAbstract {

	public AvroRecordField(String name, SchemaBuilder schema, String doc, boolean nullable, Object defaultValue, Order order) throws SchemaException {
		super(name, schema.getSchema(), doc, defaultValue, nullable, order, schema);
	}

	public AvroRecordField(String name, SchemaBuilder schema, String doc, boolean nullable, Object defaultValue) throws SchemaException {
		super(name, schema.getSchema(), doc, nullable, defaultValue, schema);
	}

	public SchemaBuilder getSchemaBuilder() {
		return schemabuilder;
	}
	
}
