package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import java.util.List;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema.Type;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;

/**
 * Wrapper around the Avro Type.MAP data type
 *
 */
public class AvroArray extends LogicalType implements IAvroPrimitive {
	public static final Factory factory = new Factory();
	public static final String NAME = "ARRAY";
	private static AvroArray element = new AvroArray();

	public static Schema getSchema(Schema schema) {
		return create().addToSchema(schema);
	}

	public AvroArray() {
		super(NAME);
	}

	public static AvroArray create() {
		return element;
	}

	@Override
	public Schema addToSchema(Schema schema) {
		return super.addToSchema(schema);
	}

	@Override
	public void validate(Schema schema) {
		super.validate(schema);
		// validate the type
		if (schema.getType() != Schema.Type.ARRAY) {
			throw new IllegalArgumentException("Logical type must be backed by an ARRAY");
		}
	}

	@Override
	public String toString() {
		return NAME;
	}

	@Override
	public Object convertToInternal(Object value) {
		return value;
	}

	public static class Factory implements LogicalTypeFactory {
		
		public Factory() {
		}

		@Override
		public LogicalType fromSchema(Schema schema) {
			return AvroArray.create();
		}

	}

	@Override
	public void toString(StringBuffer b, Object value) {
		if (value != null) {
			if (value instanceof List) {
				List<?> l = (List<?>) value;
				if (l.size() > 0) {
					b.append('[');
					boolean first = true;
					Object row1 = l.get(0);
					if (row1 instanceof Record) {
						AvroRecord recorddatatype = AvroRecord.create();
						for (Object o : l) {
							if (!first) {
								b.append(',');
							} else {
								first = false;
							}
							recorddatatype.toString(b, o);
						}
					} else {
						// This is not a perfect implementation as it renders the base Avro datatype only, e.g ByteBuffer and not BigDecimal
						for (Object o : l) {
							if (!first) {
								b.append(',');
							} else {
								first = false;
							}
							b.append(o.toString());
						}
					}
					b.append(']');
				}
			}
		}
	}

	@Override
	public Type getBackingType() {
		return Type.ARRAY;
	}

}
