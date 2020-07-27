package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import java.util.Map;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema.Type;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineCallerException;

import org.apache.avro.Schema;

/**
 * Wrapper around the Avro Type.MAP data type
 *
 */
public class AvroMap extends LogicalType implements IAvroPrimitive {
	public static final Factory factory = new Factory();
	public static final String NAME = "MAP";
	private static AvroMap element = new AvroMap();

	public static Schema getSchema(Schema schema) {
		return create().addToSchema(schema);
	}

	public AvroMap() {
		super(NAME);
	}

	public static AvroMap create() {
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
		if (schema.getType() != Schema.Type.MAP) {
			throw new IllegalArgumentException("Logical type must be backed by a MAP");
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
	public Map<?,?> convertToInternal(Object value) throws PipelineCallerException {
		if (value == null) {
			return null;
		} else if (value instanceof Map) {
			return (Map<?,?>) value;
		}
		throw new PipelineCallerException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Map");
	}

	@Override
	public Map<?,?> convertToJava(Object value) throws PipelineCallerException {
		if (value == null) {
			return null;
		} else if (value instanceof Map) {
			return (Map<?,?>) value;
		}
		throw new PipelineCallerException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Map");
	}

	public static class Factory implements LogicalTypeFactory {
		
		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroMap.create();
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
		return Type.MAP;
	}

	@Override
	public Schema getDatatypeSchema() {
		return null;
	}

	@Override
	public AvroType getAvroType() {
		return AvroType.AVROMAP;
	}

}
