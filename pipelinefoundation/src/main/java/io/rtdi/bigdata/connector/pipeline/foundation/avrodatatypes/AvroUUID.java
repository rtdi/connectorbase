package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Wrapper for LogicalTypes.uuid()
 *
 */
public class AvroUUID implements IAvroPrimitive {
	private static Schema schema = Schema.create(Type.STRING);
	
	static {
		LogicalTypes.uuid().addToSchema(schema);
	}

	public static Schema getSchema() {
		return schema;
	}

}
