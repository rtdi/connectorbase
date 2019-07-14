package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema.Type;

/**
 * Based on an INT but is supposed to hold data from -127 to +128 only. A single signed byte.
 *
 */
public class AvroByte extends LogicalType implements IAvroPrimitive {
	public static final Factory factory = new Factory();
	public static final String NAME = "BYTE";
	private static AvroByte element = new AvroByte();

	private AvroByte() {
		super(NAME);
	}
	
	public static AvroByte create() {
		return element;
	}

	@Override
	public Schema addToSchema(Schema schema) {
		super.addToSchema(schema);
		return schema;
	}

	public static Schema getSchema() {
		Schema s = Schema.create(Type.INT); 
		create().addToSchema(s);
		return s;
	}

	@Override
	public void validate(Schema schema) {
		super.validate(schema);
		// validate the type
		if (schema.getType() != Schema.Type.INT) {
			throw new IllegalArgumentException("Logical type must be backed by an integer");
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

	
	public static class Factory implements LogicalTypeFactory {
		
		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroByte.create();
		}

	}

}
