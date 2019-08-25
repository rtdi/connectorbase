package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema.Type;

/**
 * Avro Type.STRING backed datatype for URI/URL data
 *
 */
public class AvroUri extends LogicalType implements IAvroPrimitive {
	public static final Factory factory = new Factory();
	public static final String NAME = "URI";
	private static AvroUri element = new AvroUri();
	private static Schema schema;
	
	static {
		schema = create().addToSchema(Schema.create(Type.STRING));
	}

	private AvroUri() {
		super(NAME);
	}
	
	public static AvroUri create() {
		return element;
	}

	private AvroUri(Schema schema) {
		super(NAME);
	}

	@Override
	public Schema addToSchema(Schema schema) {
		return super.addToSchema(schema);
	}

	public static Schema getSchema() {
		return schema;
	}

	@Override
	public void validate(Schema schema) {
		super.validate(schema);
		// validate the type
		if (schema.getType() != Schema.Type.STRING) {
			throw new IllegalArgumentException("Logical type varchar must be backed by string");
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
			return AvroUri.create();
		}

	}

	@Override
	public Type getBackingType() {
		return Type.STRING;
	}

}
