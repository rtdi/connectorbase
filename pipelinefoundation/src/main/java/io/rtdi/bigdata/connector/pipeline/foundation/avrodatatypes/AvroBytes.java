package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Is the Avro Type.BYTES datatype, a binary store of any length. A BLOB column.
 *
 */
public class AvroBytes extends LogicalType implements IAvroPrimitive {
	public static final Factory factory = new Factory();
	public static final String NAME = "BYTES";
	private static AvroBytes element = new AvroBytes();
	private static Schema schema;

	static {
		schema = create().addToSchema(Schema.create(Type.BYTES));
	}

	public static Schema getSchema() {
		return schema;
	}
	
	public AvroBytes() {
		super(NAME);
	}
	
	public static AvroBytes create() {
		return element;
	}

	@Override
	public Schema addToSchema(Schema schema) {
		return super.addToSchema(schema);
	}

	@Override
	public void validate(Schema schema) {
		super.validate(schema);
		// validate the type
		if (schema.getType() != Schema.Type.BYTES) {
			throw new IllegalArgumentException("Logical type must be backed by BYTES");
		}
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
		return value;
	}

	public static class Factory implements LogicalTypeFactory {
		
		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroBytes.create();
		}

	}

	@Override
	public void toString(StringBuffer b, Object value) {
		if (value != null) {
			b.append('\"');
			b.append(value.toString());
			b.append('\"');
		}
	}

	@Override
	public Type getBackingType() {
		return Type.BYTES;
	}

}
