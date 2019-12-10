package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * A union of all primitive datatypes, mostly used for extensions.
 *
 */
public class AvroAnyPrimitive implements IAvroPrimitive {
	public static final String NAME = "ANYPRIMITIVE";
	private static AvroAnyPrimitive element = new AvroAnyPrimitive();
	private static Schema schema;
	
	static {
		schema = 
			Schema.createUnion(
				Schema.create(Type.NULL), 
				AvroBoolean.getSchema(),
				AvroBytes.getSchema(),
				AvroDouble.getSchema(),
				AvroFloat.getSchema(),
				AvroInt.getSchema(),
				AvroLong.getSchema(),
				AvroString.getSchema());
	}

	public static Schema getSchema() {
		return schema;
	}

	public static AvroAnyPrimitive create() {
		return element;
	}


	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		return true;
	}

	@Override
	public int hashCode() {
		return 1;
	}
	
	@Override
	public String toString() {
		return NAME;
	}

	@Override
	public Object convertToInternal(Object value) {
		if (value == null) {
			return null;
		} else {
			return value;
		}
	}

	@Override
	public void toString(StringBuffer b, Object value) {
		if (value != null) {
			b.append(value.toString());
		}
	}

	@Override
	public Type getBackingType() {
		return Type.UNION;
	}

}
