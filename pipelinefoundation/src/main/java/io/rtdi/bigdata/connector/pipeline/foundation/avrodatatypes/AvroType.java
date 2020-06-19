package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes.Decimal;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import io.rtdi.bigdata.connector.pipeline.foundation.utils.IOUtils;

/**
 * ENUM to handle data type setting properly
 *
 */
public enum AvroType {
	/**
	 * A 8bit signed integer
	 */
	AVROBYTE(0, AvroDatatypeClass.NUMBER),
	/**
	 * ASCII text of large size, comparison and sorting is binary
	 */
	AVROCLOB(2, AvroDatatypeClass.TEXTASCII),
	/**
	 * Unicode text of large size, comparison and sorting is binary
	 */
	AVRONCLOB(4, AvroDatatypeClass.TEXTUNICODE),
	/**
	 * Unicode text up t n chars long, comparison and sorting is binary
	 */
	AVRONVARCHAR(3, AvroDatatypeClass.TEXTUNICODE),
	/**
	 * A 16bit signed integer
	 */
	AVROSHORT(1, AvroDatatypeClass.NUMBER),
	/**
	 * A Spatial data type in WKT representation 
	 */
	AVROSTGEOMETRY(0, AvroDatatypeClass.TEXTASCII),
	/**
	 * A Spatial data type in WKT representation 
	 */
	AVROSTPOINT(0, AvroDatatypeClass.TEXTASCII),
	/**
	 * A string as URI
	 */
	AVROURI(0, AvroDatatypeClass.TEXTASCII),
	/**
	 * An ASCII string of n chars length, comparison and sorting is binary
	 */
	AVROVARCHAR(1, AvroDatatypeClass.TEXTASCII),
	/**
	 * A date without time information 
	 */
	AVRODATE(0, AvroDatatypeClass.DATETIME),
	/**
	 * A numeric value with precision and scale
	 */
	AVRODECIMAL(6, AvroDatatypeClass.NUMBER),
	/**
	 * A time information down to milliseconds
	 */
	AVROTIMEMILLIS(0, AvroDatatypeClass.DATETIME),
	/**
	 * A time information down to microseconds
	 */
	AVROTIMEMICROS(1, AvroDatatypeClass.DATETIME),
	/**
	 * A timestamp down to milliseconds
	 */
	AVROTIMESTAMPMILLIS(2, AvroDatatypeClass.DATETIME),
	/**
	 * A timestamp down to microseconds
	 */
	AVROTIMESTAMPMICROS(3, AvroDatatypeClass.DATETIME),
	/**
	 * Boolean
	 */
	AVROBOOLEAN(0, AvroDatatypeClass.BOOLEAN),
	/**
	 * A 32bit signed integer value
	 */
	AVROINT(2, AvroDatatypeClass.NUMBER),
	/**
	 * A 64bit signed integer value
	 */
	AVROLONG(3, AvroDatatypeClass.NUMBER),
	/**
	 * A 32bit floating point number
	 */
	AVROFLOAT(4, AvroDatatypeClass.NUMBER),
	/**
	 * A 64bit floating point number
	 */
	AVRODOUBLE(5, AvroDatatypeClass.NUMBER),
	/**
	 * Binary data of any length
	 */
	AVROBYTES(1, AvroDatatypeClass.BINARY),
	/**
	 * A unbounded unicode text - prefer using nvarchar or nclob instead to indicate its normal length, comparison and sorting is binary
	 */
	AVROSTRING(5, AvroDatatypeClass.TEXTUNICODE),
	/**
	 * A binary object with an upper size limit
	 */
	AVROFIXED(0, AvroDatatypeClass.BINARY),
	/**
	 * A unicode string with a list of allowed values - one of enum(), comparison and sorting is binary
	 */
	AVROENUM(0, AvroDatatypeClass.TEXTUNICODE),
	/**
	 * A unicode string array with a list of allowed values - many of map(), comparison and sorting is binary
	 */
	AVROMAP(0, AvroDatatypeClass.COMPLEX),
	/**
	 * An ASCII string formatted as UUID, comparison and sorting is binary
	 */
	AVROUUID(0, AvroDatatypeClass.TEXTASCII),
	/**
	 * An array of elements
	 */
	AVROARRAY(1, AvroDatatypeClass.COMPLEX),
	/**
	 * A Record of its own
	 */
	AVRORECORD(0, AvroDatatypeClass.COMPLEX),
	/**
	 * An union of multiple primitive datatypes, e.g. used for extensions
	 */
	AVROANYPRIMITIVE(99, AvroDatatypeClass.TEXTUNICODE);
	
	private int level;
	private AvroDatatypeClass group;

	AvroType(int level, AvroDatatypeClass group) {
		this.level = level;
		this.group = group;
	}

	public static AvroType getType(Schema schema) {
		LogicalType l = schema.getLogicalType();
		if (l != null) {
			switch (l.getName()) {
			case AvroByte.NAME: return AVROBYTE;
			case AvroCLOB.NAME: return AVROCLOB;
			case AvroNCLOB.NAME: return AVRONCLOB;
			case AvroNVarchar.NAME: return AVRONVARCHAR;
			case AvroShort.NAME: return AVROSHORT;
			case AvroSTGeometry.NAME: return AVROSTGEOMETRY;
			case AvroSTPoint.NAME: return AVROSTPOINT;
			case AvroUri.NAME: return AVROURI;
			case AvroVarchar.NAME: return AVROVARCHAR;
			case "date": return AVRODATE;
			case "decimal": return AVRODECIMAL;
			case "time-millis": return AVROTIMEMILLIS;
			case "time-micros": return AVROTIMEMICROS;
			case "timestamp-millis": return AVROTIMESTAMPMILLIS;
			case "timestamp-micros": return AVROTIMESTAMPMICROS;
			case "uuid": return AVROUUID;
			}
		}
		switch (schema.getType()) {
		case BOOLEAN: return AVROBOOLEAN;
		case BYTES: return AVROBYTES;
		case DOUBLE: return AVRODOUBLE;
		case ENUM: return AVROENUM;
		case FIXED: return AVROFIXED;
		case FLOAT: return AVROFLOAT;
		case INT: return AVROINT;
		case LONG: return AVROLONG;
		case MAP: return AVROMAP;
		case STRING: return AVROSTRING;
		case ARRAY: return AVROARRAY;
		case RECORD: return AVRORECORD;
		case UNION: 
			if (schema.equals(AvroAnyPrimitive.getSchema())) {
				return AVROANYPRIMITIVE;
			}
		default: return null;
		}
	}

	public static IAvroDatatype getAvroDataType(Schema schema) {
        Schema baseschema = IOUtils.getBaseSchema(schema);
		LogicalType l = baseschema.getLogicalType();
		if (l != null) {
			if (l instanceof IAvroDatatype) {
				return (IAvroDatatype) l;
			} else {
				switch (l.getName()) {
				case "date": return AvroDate.create();
				case "decimal": return AvroDecimal.create((Decimal) l);
				case "time-millis": return AvroTime.create();
				case "time-micros": return AvroTimeMicros.create();
				case "timestamp-millis": return AvroTimestamp.create();
				case "timestamp-micros": return AvroTimestampMicros.create();
				case "uuid": return AvroUUID.create();
				}
			}
		}
		switch (baseschema.getType()) {
		case BOOLEAN: return AvroBoolean.create();
		case BYTES: return AvroBytes.create();
		case DOUBLE: return AvroDouble.create();
		case ENUM: return AvroEnum.create();
		case FIXED: return AvroFixed.create(baseschema.getFixedSize());
		case FLOAT: return AvroFloat.create();
		case INT: return AvroInt.create();
		case LONG: return AvroLong.create();
		case MAP: return AvroMap.create();
		case STRING: return AvroString.create();
		case ARRAY: return AvroArray.create();
		case RECORD: return AvroRecord.create();
		case UNION: 
			if (schema.equals(AvroAnyPrimitive.getSchema())) {
				return AvroAnyPrimitive.create();
			}
		default: return null;
		}
	}

	public static String getAvroDatatype(Schema schema) {
		if (schema.getType() == Type.UNION) {
			if (schema.getTypes().size() > 2) {
				return AvroAnyPrimitive.NAME;
			} else {
				schema = IOUtils.getBaseSchema(schema);
			}
		}
		LogicalType l = schema.getLogicalType();
		if (l != null) {
			if (l instanceof LogicalTypeWithLength) {
				return l.toString();
			} else {
				return l.getName();
			}
		} else {
			return schema.getType().name();
		}
	}
	
	public static Schema getSchemaFromDataTypeRepresentation(String text) {
		switch (text) {
		case AvroBoolean.NAME: return AvroBoolean.getSchema();
		case AvroBytes.NAME: return AvroBytes.getSchema();
		case AvroDouble.NAME: return AvroDouble.getSchema();
		case AvroFloat.NAME: return AvroFloat.getSchema();
		case AvroInt.NAME: return AvroInt.getSchema();
		case AvroLong.NAME: return AvroLong.getSchema();
		case AvroString.NAME: return AvroString.getSchema();
		case AvroDate.NAME: return AvroDate.getSchema();
		case AvroTime.NAME: return AvroTime.getSchema();
		case AvroTimestamp.NAME: return AvroTimestamp.getSchema();
		case AvroUUID.NAME: return AvroUUID.getSchema();
		case AvroAnyPrimitive.NAME: return AvroAnyPrimitive.getSchema();
		case AvroByte.NAME:
			return AvroByte.getSchema();
		case AvroCLOB.NAME:
			return AvroCLOB.getSchema();
		case AvroNCLOB.NAME:
			return AvroNCLOB.getSchema();
		case AvroShort.NAME:
			return AvroShort.getSchema();
		case AvroSTGeometry.NAME:
			return AvroSTGeometry.getSchema();
		case AvroSTPoint.NAME:
			return AvroSTPoint.getSchema();
		case AvroUri.NAME:
			return AvroUri.getSchema();
		}
		if (text.startsWith(AvroDecimal.NAME)) {
			return AvroDecimal.getSchema(text);
		} else if (text.startsWith(AvroNVarchar.NAME)) {
			return AvroNVarchar.getSchema(text);
		} else if (text.startsWith(AvroVarchar.NAME)) {
			return AvroVarchar.getSchema(text);
		} else {
			return null;
		}
	}

	public static IAvroDatatype getDataTypeFromString(String text) {
		switch (text) {
		case AvroBoolean.NAME: return AvroBoolean.create();
		case AvroBytes.NAME: return AvroBytes.create();
		case AvroDouble.NAME: return AvroDouble.create();
		case AvroFloat.NAME: return AvroFloat.create();
		case AvroInt.NAME: return AvroInt.create();
		case AvroLong.NAME: return AvroLong.create();
		case AvroString.NAME: return AvroString.create();
		case AvroDate.NAME: return AvroDate.create();
		case AvroTime.NAME: return AvroTime.create();
		case AvroTimestamp.NAME: return AvroTimestamp.create();
		case AvroUUID.NAME: return AvroUUID.create();
		case AvroAnyPrimitive.NAME: return AvroAnyPrimitive.create();
		case AvroByte.NAME:
			return AvroByte.create();
		case AvroCLOB.NAME:
			return AvroCLOB.create();
		case AvroNCLOB.NAME:
			return AvroNCLOB.create();
		case AvroShort.NAME:
			return AvroShort.create();
		case AvroSTGeometry.NAME:
			return AvroSTGeometry.create();
		case AvroSTPoint.NAME:
			return AvroSTPoint.create();
		case AvroUri.NAME:
			return AvroUri.create();
		}
		if (text.startsWith(AvroDecimal.NAME)) {
			return AvroDecimal.create(text);
		} else if (text.startsWith(AvroNVarchar.NAME)) {
			return AvroNVarchar.create(text);
		} else if (text.startsWith(AvroVarchar.NAME)) {
			return AvroVarchar.create(text);
		} else {
			return null;
		}
	}

	public int getLevel() {
		return level;
	}

	public AvroDatatypeClass getGroup() {
		return group;
	}
	
	public AvroType aggregate(AvroType t) {
		if (this == t) {
			return this;
		} else if (this.getGroup() == t.getGroup()) {
			// e.g. VARCHAR --> CLOB
			if (level < t.level) {
				return t;
			} else {
				return this;
			}
		} else {
			// e.g. CLOB --> NVARCHAR
			if (this.getGroup() == AvroDatatypeClass.TEXTASCII && t.getGroup() == AvroDatatypeClass.TEXTUNICODE) {
				if (this == AVROCLOB) {
					return AVRONCLOB;
				} else {
					return t;
				}
			} else {
				// e.g. DATE --> STRING
				return AVRONVARCHAR;
			}
		}
	}
	
	public static IAvroDatatype getDataType(AvroType type, int length, int scale) {
		if (type == null) {
			return AvroNVarchar.create(100);
		} else {
			switch (type) {
			case AVROANYPRIMITIVE:
				return AvroAnyPrimitive.create();
			case AVROARRAY:
				return AvroArray.create();
			case AVROBOOLEAN:
				return AvroBoolean.create();
			case AVROBYTE:
				return AvroByte.create();
			case AVROBYTES:
				return AvroBytes.create();
			case AVROCLOB:
				return AvroCLOB.create();
			case AVRODATE:
				return AvroDate.create();
			case AVRODECIMAL:
				return AvroDecimal.create(length, scale);
			case AVRODOUBLE:
				return AvroDouble.create();
			case AVROENUM:
				return AvroEnum.create();
			case AVROFIXED:
				return AvroFixed.create(length);
			case AVROFLOAT:
				return AvroFloat.create();
			case AVROINT:
				return AvroInt.create();
			case AVROLONG:
				return AvroLong.create();
			case AVROMAP:
				return AvroMap.create();
			case AVRONCLOB:
				return AvroCLOB.create();
			case AVRONVARCHAR:
				return AvroNVarchar.create(length);
			case AVRORECORD:
				return AvroRecord.create();
			case AVROSHORT:
				return AvroShort.create();
			case AVROSTGEOMETRY:
				return AvroSTGeometry.create();
			case AVROSTPOINT:
				return AvroSTPoint.create();
			case AVROSTRING:
				return AvroString.create();
			case AVROTIMEMICROS:
				return AvroTimeMicros.create();
			case AVROTIMEMILLIS:
				return AvroTime.create();
			case AVROTIMESTAMPMICROS:
				return AvroTimestampMicros.create();
			case AVROTIMESTAMPMILLIS:
				return AvroTimestamp.create();
			case AVROURI:
				return AvroUri.create();
			case AVROUUID:
				return AvroUUID.create();
			case AVROVARCHAR:
				return AvroVarchar.create(length);
			default:
				return AvroNVarchar.create(length);
			}
		}
	}

}
