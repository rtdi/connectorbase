package io.rtdi.bigdata.connector.connectorframework.utils;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;

public class AvroPrettyPrinter {

	public static String formatRecord(GenericRecord record, int maxlength, int maxprimitivelength) {
		StringBuffer b = new StringBuffer();
		formatRecord(b, record, maxlength, maxprimitivelength, 0);
		return b.toString();
	}

	private static void formatRecord(StringBuffer b, GenericRecord record, int maxlength, int maxprimitivelength, int depth) {
		if (record != null) {
			b.append("{ ");
			List<Field> fields = record.getSchema().getFields();
			if (fields != null) {
				boolean first = true;
				for (int i = 0; i<fields.size(); i++) {
					Field field = fields.get(i);
					Object value = record.get(i);
					if (value != null) {
						if (first) {
							first = false;
						} else {
							b.append(", ");
						}
						b.append(field.name());
						b.append(" : ");
						formatField(b, field.schema(), value, maxlength, maxprimitivelength, depth);
					}
					if (b.length() >= maxlength && maxlength != -1) {
						break; // Break the for loop early and print the closing } char.
					}
				}
			}
			b.append("} ");
		}
	}
	
	private static void formatField(StringBuffer b, Schema schema, Object value, int maxlength, int maxprimitivelength, int depth) {
		if (b.length() < maxlength) {
			switch (schema.getType()) {
			case ARRAY:
				b.append("[");
				formatarray(b, (List<?>) value, schema, maxlength, maxprimitivelength, depth);
				b.append("]");
				break;
			case BYTES:
				b.append("<BLOB> ");
				break;
			case NULL:
				break;
			case RECORD:
				formatRecord(b, (GenericRecord) value, maxlength, maxprimitivelength, depth+1);
				break;
			case UNION:
				if (value != null) {
					if (value instanceof GenericRecord) {
						GenericRecord rec = (GenericRecord) value;
						formatField(b, rec.getSchema(), rec, maxlength, maxprimitivelength, depth);
					} else {
						List<Schema> types = schema.getTypes();
						if (types.size() == 2 && types.get(0).getType() == Type.NULL) {
							formatField(b, types.get(1), value, maxlength, maxprimitivelength, depth);
						} else {
							//TODO: Union of other than NULL, <TYPE>
						}
					}
				} else {
					//TODO: Common Null handling
				}
				break;
			default:
				{
					String valuestring = value.toString();
					if (valuestring.length() < maxprimitivelength) {
						b.append(valuestring);
					} else {
						b.append(valuestring.substring(0, maxprimitivelength));
						b.append("...");
					}
					return;
				}
			}
		}
	}
	
	private static void formatarray(StringBuffer b, List<?> values, Schema schema, int maxlength, int maxprimitivelength, int depth) {
		if (values != null) {
			boolean first = true;
			for (Object value : values) {
				if (first) {
					first = false;
				} else {
					b.append(", ");
				}
				formatField(b, schema.getElementType(), value, maxlength, maxprimitivelength, depth);
				if (b.length() >= maxlength && maxlength != -1) {
					return;
				}
			}
		}
	}

}
