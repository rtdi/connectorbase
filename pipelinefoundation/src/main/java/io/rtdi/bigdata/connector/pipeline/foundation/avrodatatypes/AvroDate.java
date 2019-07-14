package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Based on a Avro Type.INT holds the date portion without time.
 * Wraps the Avro LogicalTypes.date().
 *
 */
public class AvroDate implements IAvroPrimitive {
	private static Schema s = Schema.create(Type.INT);
	static {
		LogicalTypes.date().addToSchema(s);
	}

	public static Schema getSchema() {
		return s;
	}

}
