package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema.Type;

/**
 * A varchar is a string up to a provided max length, holds ASCII chars only (minus special chars like backspace) 
 * and is sorted and compared binary. 
 *
 */
public class AvroVarchar extends LogicalTypeWithLength {
	public static final Factory factory = new Factory();
	public static final String NAME = "VARCHAR";

	private AvroVarchar(int length) {
		super(NAME, length);
	}
	
	public static AvroVarchar create(int length) {
		return new AvroVarchar(length);
	}

	private static AvroVarchar create(Schema schema) {
		return new AvroVarchar(getLengthProperty(schema));
	}

	public static Schema getSchema(int length) {
		Schema s = Schema.create(Type.STRING); 
		create(length).addToSchema(s);
		return s;
	}
	
	public static Schema getSchema(String text) {
		int length = LogicalTypeWithLength.getLengthPortion(text);
		return getSchema(length);
	}

	public static class Factory implements LogicalTypeFactory {
		
		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroVarchar.create(schema);
		}

	}

}
