package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema.Type;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.AvroUtils;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineCallerException;

/**
 * A nvarchar is a string up to a provided max length, holds UTF-8 chars 
 * and is sorted and compared binary. 
 *
 */
public class AvroNVarchar extends LogicalTypeWithLength {
	public static final String NAME = "NVARCHAR";
	public static final Factory factory = new Factory();
	private Schema schema;

	private AvroNVarchar(int length) {
		super(NAME, length);
		this.schema = addToSchema(Schema.create(Type.STRING));
	}
	
	public static AvroNVarchar create(int length) {
		return new AvroNVarchar(length);
	}

	public static Schema getSchema(String text) {
		int length = LogicalTypeWithLength.getLengthPortion(text);
		return getSchema(length);
	}

	public static AvroNVarchar create(Schema schema) {
		return new AvroNVarchar(getLengthProperty(schema));
	}

	public static AvroNVarchar create(String text) {
		return new AvroNVarchar(getLengthPortion(text));
	}

	public static Schema getSchema(int length) {
		return create(length).addToSchema(Schema.create(Type.STRING));
	}
	
	@Override
	public void toString(StringBuffer b, Object value) {
		if (value != null) {
			b.append('\"');
			b.append(AvroUtils.encodeJson(value.toString()));
			b.append('\"');
		}
	}

	@Override
	public String convertToInternal(Object value) throws PipelineCallerException {
		if (value == null) {
			return null;
		} else if (value instanceof String) {
			return (String) value;
		} else {
			return value.toString();
		}
	}

	@Override
	public String convertToJava(Object value) throws PipelineCallerException {
		if (value == null) {
			return null;
		} else if (value instanceof String) {
			return (String) value;
		} else {
			return value.toString();
		}
	}

	public static class Factory implements LogicalTypeFactory {
		
		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroNVarchar.create(schema);
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
		return AvroType.AVRONVARCHAR;
	}

}
