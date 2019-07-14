package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Based on the Avro Type.BYTES data type and wraps the LogicalTypes.decimal(precision, scale).
 *
 */
public class AvroDecimal implements IAvroPrimitive {

	public static Schema getSchema(int precision, int scale) {
		Schema s = Schema.create(Type.BYTES); 
		LogicalTypes.decimal(precision, scale).addToSchema(s);
		return s;
	}

	public static Schema getSchema(String text) {
		String[] parts = text.split("[\\(\\)\\,]");
		int precision = Integer.parseInt(parts[1]);
		int scale = Integer.parseInt(parts[2]);
		return getSchema(precision, scale);
	}

}
