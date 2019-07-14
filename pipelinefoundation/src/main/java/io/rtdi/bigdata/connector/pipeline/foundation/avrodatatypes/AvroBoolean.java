package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Is the Avro Type.BOOLEAN native type.
 *
 */
public class AvroBoolean implements IAvroPrimitive {
	private static Schema schema = Schema.create(Type.BOOLEAN);

	public static Schema getSchema() {
		return schema;
	}

}
