package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Wrapper of the Avro Type.FLOAT, a 32 bit IEEE 754 floating-point number.
 *
 */
public class AvroFloat implements IAvroPrimitive {
	private static Schema schema = Schema.create(Type.FLOAT);

	public static Schema getSchema() {
		return schema;
	}
}
