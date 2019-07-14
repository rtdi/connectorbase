package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Wrapper of the Avro Type.INT
 *
 */
public class AvroInt implements IAvroPrimitive {
	private static Schema schema = Schema.create(Type.INT);

	public static Schema getSchema() {
		return schema;
	}
}
