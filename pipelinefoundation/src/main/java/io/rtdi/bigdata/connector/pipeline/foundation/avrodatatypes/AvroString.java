package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Wrapper around the Avro Type.STRING data type
 *
 */
public class AvroString implements IAvroPrimitive {

	private static Schema schema = Schema.create(Type.STRING);
	
	public static Schema getSchema() {
		return schema;
	}
}
