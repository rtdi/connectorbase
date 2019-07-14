package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

public abstract class LogicalTypeWithLength extends LogicalType implements IAvroPrimitive {
	static final String LENGTH_PROP = "length";

	private int length;

	public LogicalTypeWithLength(String name, int length) {
		super(name);
		this.length = length;
	}

	public int getLength() {
		return length;
	}
	
	public static Integer getLengthProperty(Schema schema) {
		Object p = schema.getObjectProp(LENGTH_PROP);
		if (p == null) {
			throw new IllegalArgumentException(
					"Schema is missing the length property");
		} else if (p instanceof Integer) {
			return (Integer) p;
		} else if (p instanceof String) {
			try {
				return Integer.valueOf(p.toString());
			} catch (NumberFormatException e) {
			}
		}
		throw new IllegalArgumentException("Expected an integer for length property but got \"" + p.toString() + "\"");
	}

	@Override
	public void validate(Schema schema) {
		super.validate(schema);
		// validate the type
		if (schema.getType() != Schema.Type.STRING) {
			throw new IllegalArgumentException("Logical type " + getName() + " must be backed by string");
		}
		if (length <= 0) {
			throw new IllegalArgumentException("Invalid length: " + length + " (must be positive)");
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		LogicalTypeWithLength typewithlength = (LogicalTypeWithLength) o;

		if (length != typewithlength.length) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return Integer.hashCode(length);
	}

	@Override
	public Schema addToSchema(Schema schema) {
		super.addToSchema(schema);
		schema.addProp(LENGTH_PROP, length);
		return schema;
	}

	@Override
	public String toString() {
		return getName() + "(" + length + ")";
	}

	public static int getLengthPortion(String text) {
		int i = text.indexOf('(');
		int j = text.indexOf(')');
		String l = text.substring(i+1, j);
		return Integer.valueOf(l);
	}
}
