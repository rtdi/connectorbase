package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Is the Avro Type.BOOLEAN native type.
 *
 */
public class AvroBoolean extends LogicalType implements IAvroPrimitive {
	public static final Factory factory = new Factory();
	public static final String NAME = "BOOLEAN";
	private static AvroBoolean element = new AvroBoolean();
	private static Schema schema;

	static {
		schema = create().addToSchema(Schema.create(Type.BOOLEAN));
	}

	public static Schema getSchema() {
		return schema;
	}

	public AvroBoolean() {
		super(NAME);
	}

	public static AvroBoolean create() {
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
		if (schema.getType() != Schema.Type.BOOLEAN) {
			throw new IllegalArgumentException("Logical type must be backed by a BOOLEAN");
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
			return AvroBoolean.create();
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
		return Type.BOOLEAN;
	}

}
