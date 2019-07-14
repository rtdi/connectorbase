package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Wrapper of the Avro Type.DOUBLE, a 64 bit IEEE 754 floating-point number.
 *
 */
public class AvroDouble implements IAvroPrimitive {
	private static Schema schema = Schema.create(Type.DOUBLE);

	public static Schema getSchema() {
		return schema;
	}
}
