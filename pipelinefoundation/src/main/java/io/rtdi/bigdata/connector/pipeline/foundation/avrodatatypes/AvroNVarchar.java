package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema.Type;

/**
 * A nvarchar is a string up to a provided max length, holds UTF-8 chars 
 * and is sorted and compared binary. 
 *
 */
public class AvroNVarchar extends LogicalTypeWithLength {
	public static final String NAME = "NVARCHAR";
	public static final Factory factory = new Factory();

	private AvroNVarchar(int length) {
		super(NAME, length);
	}
	
	public static AvroNVarchar create(int length) {
		return new AvroNVarchar(length);
	}

	public static Schema getSchema(String text) {
		int length = LogicalTypeWithLength.getLengthPortion(text);
		return getSchema(length);
	}

	private static AvroNVarchar create(Schema schema) {
		return new AvroNVarchar(getLengthProperty(schema));
	}
	
	public static Schema getSchema(int length) {
		return create(length).addToSchema(Schema.create(Type.STRING));
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
			return AvroNVarchar.create(schema);
		}

	}

	@Override
	public Type getBackingType() {
		return Type.STRING;
	}

}
