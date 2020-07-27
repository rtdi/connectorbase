package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.LogicalTypes.TimeMillis;
import org.apache.avro.Schema.Type;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineCallerException;

/**
 * Wrapper of LogicalTypes.timeMillis()
 *
 */
public class AvroTime extends LogicalType implements IAvroPrimitive {
	public static final Factory factory = new Factory();
	private static Schema schema;
	public static final String NAME = "TIME";
	private static AvroTime element = new AvroTime();
	private TimeMillis time = LogicalTypes.timeMillis();

	static {
		schema = LogicalTypes.timeMillis().addToSchema(Schema.create(Type.INT));
	}

	public static Schema getSchema() {
		return schema;
	}

	public AvroTime() {
		super(NAME);
	}

	public static AvroTime create() {
		return element;
	}

	@Override
	public Schema addToSchema(Schema schema) {
		return super.addToSchema(schema);
	}

	@Override
	public void validate(Schema schema) {
		time.validate(schema);
	}

	@Override
	public boolean equals(Object o) {
		return time.equals(o);
	}

	@Override
	public int hashCode() {
		return time.hashCode();
	}
	
	@Override
	public String toString() {
		return NAME;
	}

	@Override
	public Object convertToInternal(Object value) throws PipelineCallerException {
		if (value == null) {
			return null;
		} else if (value instanceof Integer) {
			return value;
		}
		throw new PipelineCallerException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Time");
	}

	@Override
	public Integer convertToJava(Object value) throws PipelineCallerException {
		if (value == null) {
			return null;
		} else if (value instanceof Integer) {
			return (Integer) value;
		}
		throw new PipelineCallerException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a Time");
	}

	public static class Factory implements LogicalTypeFactory {
		
		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroTime.create();
		}

	}

	@Override
	public void toString(StringBuffer b, Object value) {
		if (value != null) {
			if (value instanceof Integer) {
				Instant time = Instant.ofEpochMilli((Integer) value);
				b.append('\"');
				b.append(LocalDateTime.ofInstant(time, ZoneOffset.UTC).format(DateTimeFormatter.ISO_TIME));
				b.append('\"');
			}
		}
	}

	@Override
	public Type getBackingType() {
		return Type.INT;
	}

	@Override
	public Schema getDatatypeSchema() {
		return schema;
	}

	@Override
	public AvroType getAvroType() {
		return AvroType.AVROTIMEMILLIS;
	}

}
