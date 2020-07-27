package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema.Type;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineCallerException;

/**
 * Wrapper of the Avro Type.LONG
 *
 */
public class AvroLong extends LogicalType implements IAvroPrimitive {
	public static final Factory factory = new Factory();
	public static final String NAME = "LONG";
	private static AvroLong element = new AvroLong();
	private static Schema schema;

	static {
		schema = create().addToSchema(Schema.create(Type.LONG));
	}

	public static Schema getSchema() {
		return schema;
	}

	public AvroLong() {
		super(NAME);
	}

	public static AvroLong create() {
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
		if (schema.getType() != Schema.Type.LONG) {
			throw new IllegalArgumentException("Logical type must be backed by a LONG");
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
	public Long convertToInternal(Object value) throws PipelineCallerException {
		if (value == null) {
			return null;
		} else if (value instanceof Long) {
			return (Long) value;
		} else if (value instanceof String) {
			try {
				return Long.valueOf((String) value);
			} catch (NumberFormatException e) {
				throw new PipelineCallerException("Cannot convert the string \"" + value + "\" into a Decimal");
			}
		} else if (value instanceof Number) {
			return ((Number) value).longValue();
		}
		throw new PipelineCallerException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Long");
	}

	@Override
	public Long convertToJava(Object value) throws PipelineCallerException {
		if (value == null) {
			return null;
		} else if (value instanceof Long) {
			return (Long) value;
		}
		throw new PipelineCallerException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Long");
	}

	public static class Factory implements LogicalTypeFactory {
		
		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroLong.create();
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
		return Type.LONG;
	}

	@Override
	public Schema getDatatypeSchema() {
		return schema;
	}

	@Override
	public AvroType getAvroType() {
		return AvroType.AVROLONG;
	}

}
