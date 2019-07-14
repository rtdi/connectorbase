package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Is the Avro Type.BYTES datatype, a binary store of any length. A BLOB column.
 *
 */
public class AvroBytes implements IAvroPrimitive {

	public static Schema getSchema() {
		return Schema.create(Type.BYTES);
	}
}
