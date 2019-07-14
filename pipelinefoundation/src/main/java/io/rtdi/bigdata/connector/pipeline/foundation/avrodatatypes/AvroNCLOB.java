package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema.Type;

/**
 * An AvroNCLOB is an unbounded large UTF-8 character string.
 * @see AvroNVarchar
 *
 */
public class AvroNCLOB extends LogicalType implements IAvroPrimitive {
	public static final Factory factory = new Factory();
	public static final String NAME = "NCLOB";
	private static AvroNCLOB element = new AvroNCLOB();

	private AvroNCLOB() {
		super(NAME);
	}
	
	public static AvroNCLOB create() {
		return element;
	}

	@Override
	public Schema addToSchema(Schema schema) {
		super.addToSchema(schema);
		return schema;
	}

	public static Schema getSchema() {
		Schema s = Schema.create(Type.STRING); 
		create().addToSchema(s);
		return s;
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

	
	public static class Factory implements LogicalTypeFactory {
		
		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroNCLOB.create();
		}

	}

}
