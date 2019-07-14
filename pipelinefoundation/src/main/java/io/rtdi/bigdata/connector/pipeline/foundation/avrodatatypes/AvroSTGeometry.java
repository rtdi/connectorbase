package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema.Type;

/**
 * Type.STRING backed representation of geospatial data in WKT format.
 *
 */
public class AvroSTGeometry extends LogicalType implements IAvroPrimitive {
	public static final Factory factory = new Factory();
	public static final String NAME = "ST_GEOMETRY";
	private static AvroSTGeometry element = new AvroSTGeometry();

	private AvroSTGeometry() {
		super(NAME);
	}
	
	public static AvroSTGeometry create() {
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
			return AvroSTGeometry.create();
		}

	}

}
