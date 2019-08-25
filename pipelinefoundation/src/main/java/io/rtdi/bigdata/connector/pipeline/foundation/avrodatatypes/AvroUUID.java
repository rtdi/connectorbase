package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema.Type;

/**
 * Wrapper for LogicalTypes.uuid()
 *
 */
public class AvroUUID extends LogicalType implements IAvroPrimitive {
	public static final Factory factory = new Factory();
	private static Schema schema;
	private static AvroUUID element = new AvroUUID();
	public static final String NAME = "UUID";

	static {
		schema = LogicalTypes.uuid().addToSchema(Schema.create(Type.STRING));
	}

	public static AvroUUID create() {
		return element;
	}

	public static Schema getSchema() {
		return schema;
	}
	
	private AvroUUID() {
		super(NAME);
	}


	@Override
	public Schema addToSchema(Schema schema) {
		super.addToSchema(schema);
		return schema;
	}

	@Override
	public void validate(Schema schema) {
		super.validate(schema);
		// validate the type
		if (schema.getType() != Schema.Type.STRING) {
			throw new IllegalArgumentException("Logical type must be backed by a string");
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
	public void toString(StringBuffer b, Object value) {
		if (value != null) {
			b.append('\"');
			b.append(value.toString());
			b.append('\"');
		}
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
			return AvroUUID.create();
		}

	}

	@Override
	public Type getBackingType() {
		return Type.STRING;
	}

}
