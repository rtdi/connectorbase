package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;

import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.Decimal;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Based on the Avro Type.BYTES data type and wraps the LogicalTypes.decimal(precision, scale).
 *
 */
public class AvroDecimal extends LogicalType implements IAvroPrimitive {
	public static final Factory factory = new Factory();
	private static final DecimalConversion DECIMAL_CONVERTER = new DecimalConversion();
	public static final String NAME = "DECIMAL";
	private Decimal decimal;
	private Schema schema;

	public static Schema getSchema(int precision, int scale) {
		return create(precision, scale).addToSchema(Schema.create(Type.BYTES));
	}

	public static Schema getSchema(String text) {
		String[] parts = text.split("[\\(\\)\\,]");
		int precision = 28;
		int scale = 7;
		if (parts.length > 1) {
			precision = Integer.parseInt(parts[1]);
		}
		if (parts.length > 2) {
			scale = Integer.parseInt(parts[2]);
		}
		return getSchema(precision, scale);
	}
	
	public static AvroDecimal create(Schema schema) {
		return new AvroDecimal(schema);
	}

	public static AvroDecimal create(Decimal l) {
		return new AvroDecimal(l);
	}

	public static AvroDecimal create(int precision, int scale) {
		return new AvroDecimal(precision, scale);
	}

	public static AvroDecimal create(String text) {
		String[] parts = text.split("[\\(\\)\\,]");
		int precision = 28;
		int scale = 7;
		if (parts.length > 1) {
			precision = Integer.parseInt(parts[1]);
		}
		if (parts.length > 2) {
			scale = Integer.parseInt(parts[2]);
		}
		return new AvroDecimal(precision, scale);
	}

	public AvroDecimal(int precision, int scale) {
		super(NAME);
		decimal = LogicalTypes.decimal(precision, scale);
		this.schema = decimal.addToSchema(Schema.create(Type.BYTES));
	}

	public AvroDecimal(Schema schema) {
		super(NAME);
		decimal = (Decimal) LogicalTypes.fromSchema(schema);
		this.schema = schema;
	}

	public AvroDecimal(Decimal l) {
		super(NAME);
		decimal = l;
		this.schema = l.addToSchema(Schema.create(Type.BYTES));
	}

	@Override
	public Schema addToSchema(Schema schema) {
		return decimal.addToSchema(schema);
	}

	@Override
	public void validate(Schema schema) {
		decimal.validate(schema);
	}

	@Override
	public boolean equals(Object o) {
		return decimal.equals(o);
	}

	@Override
	public int hashCode() {
		return decimal.hashCode();
	}
	
	@Override
	public String toString() {
		return NAME + "(" + decimal.getPrecision() + "," + decimal.getScale() + ")";
	}

	@Override
	public Object convertToInternal(Object value) {
		BigDecimal v = null;
		if (value == null) {
			return null;
		} else {
			if (value instanceof ByteBuffer || value instanceof byte[]) {
				return value;
			} else if (value instanceof BigDecimal) {
				if (decimal.getScale() != ((BigDecimal) value).scale()) {
					v = ((BigDecimal) value).setScale(decimal.getScale(), RoundingMode.HALF_UP);
				} else {
					v = (BigDecimal) value;
				}
			} else if (value instanceof Number) {
				Number n = (Number) value;
				v = BigDecimal.valueOf(Double.valueOf( n.doubleValue() )).setScale(decimal.getScale());
			} else {
				return value;
			}
			ByteBuffer buffer = DECIMAL_CONVERTER.toBytes(v, null, decimal);
			return buffer;
		}
	}

	public static class Factory implements LogicalTypeFactory {
		
		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroDecimal.create(schema);
		}

	}

	@Override
	public void toString(StringBuffer b, Object value) {
		if (value != null) {
			if (value instanceof ByteBuffer) {
				ByteBuffer v = (ByteBuffer) value;
				if (v.capacity() != 0) {
					v.position(0);
					BigDecimal n = DECIMAL_CONVERTER.fromBytes(v, null, decimal);
					v.position(0);
					b.append(n.toString());
				}
			}
		}
	}

	@Override
	public Type getBackingType() {
		return Type.BYTES;
	}

	@Override
	public Schema getDatatypeSchema() {
		return schema;
	}

	@Override
	public AvroType getAvroType() {
		return AvroType.AVRODECIMAL;
	}

}
