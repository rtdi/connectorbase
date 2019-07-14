package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Wrapper of LogicalTypes.timestampMillis()
 *
 */
public class AvroTimestamp implements IAvroPrimitive {
	private static Schema s = Schema.create(Type.LONG);
	static {
		LogicalTypes.timestampMillis().addToSchema(s);
	}

	public static Schema getSchema() {
		return s;
	}

}
