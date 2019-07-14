package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * A union of all primitive datatypes, mostly used for extensions.
 *
 */
public class AvroAnyPrimitive {
	public static final String NAME = "ANYPRIMITIVE";
	private static Schema schema = Schema.createUnion(
			Schema.create(Type.NULL), 
			AvroBoolean.getSchema(),
			AvroBytes.getSchema(),
			AvroDouble.getSchema(),
			AvroFloat.getSchema(),
			AvroInt.getSchema(),
			AvroLong.getSchema(),
			AvroString.getSchema());

	public static Schema getSchema() {
		return schema;
	}
}
