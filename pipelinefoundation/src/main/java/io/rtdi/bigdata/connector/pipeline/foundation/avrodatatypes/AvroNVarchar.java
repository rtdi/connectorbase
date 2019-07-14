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
		Schema s = Schema.create(Type.STRING); 
		create(length).addToSchema(s);
		return s;
	}
	

	public static class Factory implements LogicalTypeFactory {
		
		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroNVarchar.create(schema);
		}

	}

}
