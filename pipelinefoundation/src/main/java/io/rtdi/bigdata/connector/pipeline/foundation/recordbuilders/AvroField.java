package io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import io.rtdi.bigdata.connector.pipeline.foundation.enums.ContentSensitivity;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.AvroNameEncoder;

public class AvroField extends Field {

	public static final String COLUMN_PROP_SOURCEDATATYPE = "__source_data_type";
	public static final String COLUMN_PROP_ORIGINALNAME = "__originalname";
	public static final String COLUMN_PROP_PRIMARYKEY = "__primary_key_column";
	public static final String COLUMN_PROP_INTERNAL = "__internal";
	public static final String COLUMN_PROP_TECHNICAL = "__technical"; // cannot be used in mappings as the values are set when sending the rows to the pipeline server
	public static final String COLUMN_PROP_CONTENT_SENSITIVITY = "__sensitivity";

	public AvroField(String name, Schema schema, String doc, boolean nullable, Object defaultValue) {
		super(AvroNameEncoder.encodeName(name), getSchema(schema, nullable), doc, defaultValue);
		setOriginalName(name);
	}

	public AvroField(String name, Schema schema, String doc, Object defaultValue, boolean nullable, Order order) {
		super(AvroNameEncoder.encodeName(name), getSchema(schema, nullable), doc, defaultValue, order);
		setOriginalName(name);
	}
	
	/**
	 * Create a new column with the contents based on a compiled schema field.
	 * Used to derive the key schema from the value schema.
	 * 
	 * @param f as Avro's field object
	 */
	public AvroField(Field f) {
		super(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order());
		setOriginalName(getOriginalName(f));
	}

	
	protected static Schema getSchema(Schema schema, boolean nullable) {
		if (nullable) {
			return Schema.createUnion(Schema.create(Type.NULL), schema);
		} else {
			return schema;
		}
	}

	public AvroField setPrimaryKey() {
		addProp(COLUMN_PROP_PRIMARYKEY, Boolean.TRUE);
		return this;
	}
	
	public boolean isPrimaryKey() {
		return isPrimaryKey(this);
	}
	
	public AvroField setSourceDataType(String sourcedatatype) {
		addProp(COLUMN_PROP_SOURCEDATATYPE, sourcedatatype);
		return this;
	}
	
	public String getSourceDataType() {
		return getSourceDataType(this);
	}
	
	public AvroField setSensitivity(ContentSensitivity sensitivity) {
		addProp(COLUMN_PROP_CONTENT_SENSITIVITY, sensitivity.name());
		return this;
	}
	
	public ContentSensitivity getSensitivity() {
		return getContentSensitivity(this);
	}
	
	private void setOriginalName(String name) {
		addProp(COLUMN_PROP_ORIGINALNAME, name);
	}
	
	public String getOriginalName() {
		return getOriginalName(this);
	}
	
	public AvroField setInternal() {
		addProp(COLUMN_PROP_INTERNAL, Boolean.TRUE);
		return this;
	}

	public boolean isInternal() {
		return isInternal(this);
	}

	public AvroField setTechnical() {
		addProp(COLUMN_PROP_TECHNICAL, Boolean.TRUE);
		return this;
	}

	public boolean isTechnical() {
		return isTechnical(this);
	}

	public static boolean isPrimaryKey(Field field) {
		Object pk = field.getObjectProp(COLUMN_PROP_PRIMARYKEY);
		if (pk != null && pk instanceof Boolean) {
			return (boolean) pk;
		} else {
			return false;
		}
	}
	
	public static String getOriginalName(Field field) {
		Object name = field.getObjectProp(COLUMN_PROP_ORIGINALNAME);
		if (name != null && name instanceof String) {
			return (String) name;
		} else {
			return field.name();
		}
	}
	
	public static String getSourceDataType(Field field) {
		Object type = field.getObjectProp(COLUMN_PROP_SOURCEDATATYPE);
		if (type != null && type instanceof String) {
			return (String) type;
		} else {
			return null;
		}
	}

	public static boolean isInternal(Field field) {
		Object isinternal = field.getObjectProp(COLUMN_PROP_INTERNAL);
		if (isinternal != null && isinternal instanceof Boolean) {
			return (boolean) isinternal;
		} else {
			return false;
		}
	}
	
	public static boolean isTechnical(Field field) {
		Object istechnical = field.getObjectProp(COLUMN_PROP_TECHNICAL);
		if (istechnical != null && istechnical instanceof Boolean) {
			return (boolean) istechnical;
		} else {
			return false;
		}
	}
	
	public static ContentSensitivity getContentSensitivity(Field field) {
		Object type = field.getObjectProp(COLUMN_PROP_CONTENT_SENSITIVITY);
		if (type != null && type instanceof String) {
			try {
				return ContentSensitivity.valueOf((String) type);
			} catch (IllegalArgumentException e) {
				return ContentSensitivity.PUBLIC;
			}
		} else {
			return ContentSensitivity.PUBLIC;
		}
	}

}
