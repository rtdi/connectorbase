package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Wrapper of the Avro Type.LONG
 *
 */
public class AvroLong implements IAvroPrimitive {
	private static Schema schema = Schema.create(Type.LONG);

	public static Schema getSchema() {
		return schema;
	}
}
