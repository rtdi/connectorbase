package io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;

public class AvroArray extends AvroField {

	public static final String COLUMN_PROP_MIN = "__min";
	public static final String COLUMN_PROP_MAX = "__max";

	private Schema schema;

	public AvroArray(String name, Schema schema, String doc, boolean nullable, Object defaultValue, Order order) throws SchemaException {
		super(name, Schema.createArray(schema), doc, defaultValue, nullable, order);
		this.schema = schema;
	}

	public AvroArray(String name, Schema schema, String doc, boolean nullable, Object defaultValue) throws SchemaException {
		super(name, Schema.createArray(schema), doc, nullable, defaultValue);
		this.schema = schema;
	}

	public Schema getArrayElementSchema() {
		return schema;
	}

	/**
	 * This setting allows database writers to optimize the handling. For example an address might have multiple lines
	 * hence it is an array of strings. But if we can agree that there will be never more than four lines, the database table
	 * could store it in up to four columns instead of using a separate table.<br/>
	 * null means unbounded
	 * 0 means zero entries are allowed and hence can be used for min only
	 * 1 means that at least or at most one entry is needed if the array itself is present
	 * 
	 * @param min allowed occurrences, null, 0 or any positive int
	 * @param max allowed occurrences, null, 1 or any larger positive int 
	 * @return
	 * @throws SchemaException if min>max or any other illegal value for min or max
	 */
	public AvroArray setMinMax(Integer min, Integer max) throws SchemaException {
		if (min != null) {
			if (min < 0) {
				throw new SchemaException("Min has to be apositive int or null, got \"" + String.valueOf(min) + "\"");
			}
			addProp(COLUMN_PROP_MIN, min);
		}
		if (max != null) {
			if (max < 1) {
				throw new SchemaException("Min has to be apositive int or null, got \"" + String.valueOf(max) + "\"");
			} else if (min != null && min > max) {
				throw new SchemaException("Min \"=" + String.valueOf(min) + "\" is greater than max \"=" + String.valueOf(max) + "\"");
			}
			addProp(COLUMN_PROP_MAX, max);
		}
		return this;
	}
	
	public static Integer getMin(Schema schema) {
		Object o = schema.getObjectProp(COLUMN_PROP_MIN);
		if (o instanceof Integer) {
			return (Integer) o;
		} else {
			return null;
		}
	}

	public static Integer getMax(Schema schema) {
		Object o = schema.getObjectProp(COLUMN_PROP_MAX);
		if (o instanceof Integer) {
			return (Integer) o;
		} else {
			return null;
		}
	}
}
