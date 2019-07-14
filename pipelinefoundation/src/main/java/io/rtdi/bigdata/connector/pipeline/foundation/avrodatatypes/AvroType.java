package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import io.rtdi.bigdata.connector.pipeline.foundation.IOUtils;

/**
 * ENUM to handle data type setting properly
 *
 */
public enum AvroType {
	/**
	 * A 8bit signed integer
	 */
	AVROBYTE,
	/**
	 * ASCII text of large size, comparison and sorting is binary
	 */
	AVROCLOB,
	/**
	 * Unicode text of large size, comparison and sorting is binary
	 */
	AVRONCLOB,
	/**
	 * Unicode text up t n chars long, comparison and sorting is binary
	 */
	AVRONVARCHAR,
	/**
	 * A 16bit signed integer
	 */
	AVROSHORT,
	/**
	 * A Spatial data type in WKT representation 
	 */
	AVROSTGEOMETRY,
	/**
	 * A Spatial data type in WKT representation 
	 */
	AVROSTPOINT,
	/**
	 * A string as URI
	 */
	AVROURI,
	/**
	 * An ASCII string of n chars length, comparison and sorting is binary
	 */
	AVROVARCHAR,
	/**
	 * A date without time information 
	 */
	AVRODATE,
	/**
	 * A numeric value with precision and scale
	 */
	AVRODECIMAL,
	/**
	 * A time information down to milliseconds
	 */
	AVROTIMEMILLIS,
	/**
	 * A time information down to microseconds
	 */
	AVROTIMEMICROS,
	/**
	 * A timestamp down to milliseconds
	 */
	AVROTIMESTAMPMILLIS,
	/**
	 * A timestamp down to microseconds
	 */
	AVROTIMESTAMPMICROS,
	/**
	 * Boolean
	 */
	AVROBOOLEAN,
	/**
	 * A 32bit signed integer value
	 */
	AVROINT,
	/**
	 * A 64bit signed integer value
	 */
	AVROLONG,
	/**
	 * A 32bit floating point number
	 */
	AVROFLOAT,
	/**
	 * A 64bit floating point number
	 */
	AVRODOUBLE,
	/**
	 * Binary data of any length
	 */
	AVROBYTES,
	/**
	 * A unbounded unicode text - prefer using nvarchar or nclob instead to indicate its normal length, comparison and sorting is binary
	 */
	AVROSTRING,
	/**
	 * A binary object with an upper size limit
	 */
	AVROFIXED,
	/**
	 * A unicode string with a list of allowed values - one of enum(), comparison and sorting is binary
	 */
	AVROENUM,
	/**
	 * A unicode string array with a list of allowed values - many of map(), comparison and sorting is binary
	 */
	AVROMAP,
	/**
	 * An ASCII string formatted as UUID, comparison and sorting is binary
	 */
	AVROUUID,
	/**
	 * An array of elements
	 */
	AVROARRAY,
	/**
	 * A Record of its own
	 */
	AVRORECORD;
	
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
		case "boolean": return AvroBoolean.getSchema();
		case "bytes": return AvroBytes.getSchema();
		case "double": return AvroDouble.getSchema();
		case "float": return AvroFloat.getSchema();
		case "int": return AvroInt.getSchema();
		case "long": return AvroLong.getSchema();
		case "string": return AvroString.getSchema();
		case "date": return AvroDate.getSchema();
		case "time-millis": return AvroTime.getSchema();
		case "timestamp-millis": return AvroTimestamp.getSchema();
		case "uuid": return AvroUUID.getSchema();
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
		if (text.startsWith("decimal")) {
			return AvroDecimal.getSchema(text);
		} else if (text.startsWith(AvroNVarchar.NAME)) {
			return AvroNVarchar.getSchema(text);
		} else if (text.startsWith(AvroVarchar.NAME)) {
			return AvroVarchar.getSchema(text);
		} else {
			return null;
		}
	}
}
