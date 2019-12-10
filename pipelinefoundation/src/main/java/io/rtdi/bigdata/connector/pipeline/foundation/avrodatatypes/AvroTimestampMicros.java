package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import java.sql.Timestamp;
import java.util.Date;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.LogicalTypes.TimestampMicros;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * Wrapper of LogicalTypes.timestampMillis()
 *
 */
public class AvroTimestampMicros extends LogicalType implements IAvroPrimitive {
	public static final Factory factory = new Factory();
	private static Schema schema;
	static {
		schema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Type.LONG));
	}
	public static final String NAME = "TIMESTAMPMICROS";
	private static AvroTimestampMicros element = new AvroTimestampMicros();
	private TimestampMicros time = LogicalTypes.timestampMicros();

	public static Schema getSchema() {
		return schema;
	}

	public AvroTimestampMicros() {
		super(NAME);
	}

	public static AvroTimestampMicros create() {
		return element;
	}

	@Override
	public Schema addToSchema(Schema schema) {
		return super.addToSchema(schema);
	}

	@Override
	public void validate(Schema schema) {
		time.validate(schema);
	}

	@Override
	public boolean equals(Object o) {
		return time.equals(o);
	}

	@Override
	public int hashCode() {
		return time.hashCode();
	}
	
	@Override
	public String toString() {
		return NAME;
	}

	@Override
	public Object convertToInternal(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof Long) {
			return value;
		} else if (value instanceof Date) {
			return ((Date) value).getTime();
		} else {
			return value;
		}
	}

	public static class Factory implements LogicalTypeFactory {
		
		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroTimestampMicros.create();
		}

	}

	@Override
	public void toString(StringBuffer b, Object value) {
		if (value != null) {
			if (value instanceof Long) {
				Date d = new Date((Long) value);
				b.append('\"');
				b.append(d.toString());
				b.append('\"');
			}
		}
	}

	@Override
	public Type getBackingType() {
		return Type.LONG;
	}

}
