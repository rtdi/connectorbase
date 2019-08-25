package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;

public class AvroRecord implements IAvroDatatype {
	static AvroRecord element = new AvroRecord();

	private AvroRecord() {
	}

	public static AvroRecord create() {
		return element;
	}
	
	@Override
	public void toString(StringBuffer b, Object value) {
		if (value instanceof Record) {
			Record r = (Record) value;
			Schema schema = r.getSchema();
			b.append('{');
			boolean first = true;
			for (Field f : schema.getFields()) {
				Object v = r.get(f.pos());
				if (v != null) {
					IAvroDatatype datatype = AvroType.getAvroDataType(f.schema());
					if (datatype != null) {
						if (!first) {
							b.append(',');
						} else {
							first = false;
						}
						b.append('\"');
						b.append(f.name());
						b.append("\":");
						datatype.toString(b, v);
					}
				}
			}
			b.append('}');
		}
	}

	@Override
	public Object convertToInternal(Object value) {
		return value;
	}

	@Override
	public Type getBackingType() {
		return Type.RECORD;
	}

}
