package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema.Type;

/**
 * Type.STRING backed representation of geospatial data in WKT format.
 *
 */
public class AvroSTPoint extends LogicalType implements IAvroPrimitive {
	public static final Factory factory = new Factory();
	public static final String NAME = "ST_POINT";
	private static AvroSTPoint element = new AvroSTPoint();
	private static Schema schema;
	
	static {
		schema = create().addToSchema(Schema.create(Type.STRING));
	}

	public static Schema getSchema() {
		return schema;
	}

	private AvroSTPoint() {
		super(NAME);
	}
	
	public static AvroSTPoint create() {
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
			return AvroSTPoint.create();
		}

	}

	@Override
	public Type getBackingType() {
		return Type.STRING;
	}

	@Override
	public Schema getDatatypeSchema() {
		return schema;
	}


	@Override
	public AvroType getAvroType() {
		return AvroType.AVROSTPOINT;
	}

}
