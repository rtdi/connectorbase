package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Wrapper of LogicalTypes.timeMillis()
 *
 */
public class AvroTime implements IAvroPrimitive {
	private static Schema schema = Schema.create(Type.INT);
	
	static {
		LogicalTypes.timeMillis().addToSchema(schema);
	}

	public static Schema getSchema() {
		return schema;
	}

}
