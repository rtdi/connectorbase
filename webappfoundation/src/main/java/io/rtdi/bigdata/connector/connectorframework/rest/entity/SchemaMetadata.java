package io.rtdi.bigdata.connector.connectorframework.rest.entity;

import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class SchemaMetadata {
	public static final int UNDEFINED_INT = -1;

	private String keyschema;
	private String valueschema;
	private String schemaname;
	private int valueschemaid = UNDEFINED_INT;
	private int keyschemaid = UNDEFINED_INT;
	private int keyschemaversion = UNDEFINED_INT;
	private int valueschemaversion = UNDEFINED_INT;
	
	public SchemaMetadata() {
	}

	public SchemaMetadata(String schemaname) {
		this.schemaname = schemaname;
	}

	public SchemaMetadata(SchemaHandler m) throws PropertiesException {
		this(m.getSchemaName().toString());
		keyschema = m.getKeySchema().toString();
		valueschema = m.getValueSchema().toString();
		valueschemaid = m.getDetails().getValueSchemaID();
		keyschemaid = m.getDetails().getKeySchemaID();
	}

	public String getKeySchema() {
		return keyschema;
	}

	public void setKeySchema(String keyschema) {
		this.keyschema = keyschema;
	}

	public String getValueSchema() {
		return valueschema;
	}

	public void setValueSchema(String valueschema) {
		this.valueschema = valueschema;
	}

	public String getSchemaname() {
		return schemaname;
	}

	public void setSchemaname(String schemaname) {
		this.schemaname = schemaname;
	}

	@Override
	public int hashCode() {
		return getValueSchema().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof SchemaMetadata) {
			SchemaMetadata o = (SchemaMetadata) obj;
			return getValueSchema().equals(o.getValueSchema()) && keyschema.equals(o.keyschema);
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		StringBuffer b = new StringBuffer();
		b.append("keyschema: ");
		if (keyschema != null) {
			if (keyschema.length() > 20) {
				b.append(keyschema.substring(0, 20));
				b.append("...");
			} else {
				b.append(keyschema);
			}
		} else {
			b.append("null");
		}
		b.append(", valueschema: ");
		if (getValueSchema() != null) {
			if (getValueSchema().length() > 20) {
				b.append(getValueSchema().substring(0, 20));
				b.append("...");
			} else {
				b.append(getValueSchema());
			}
		} else {
			b.append("null");
		}
		
		return b.toString();
	}

	public int getValueSchemaVersion() {
		return valueschemaversion;
	}

	public void setValueSchemaVersion(int valueschemaversion) {
		this.valueschemaversion = valueschemaversion;
	}

	public int getKeySchemaVersion() {
		return keyschemaversion;
	}

	public void setKeySchemaVersion(int keyschemaversion) {
		this.keyschemaversion = keyschemaversion;
	}

	public int getKeySchemaID() {
		return keyschemaid;
	}

	public void setKeySchemaID(int keyschemaid) {
		this.keyschemaid = keyschemaid;
	}

	public int getValueSchemaID() {
		return valueschemaid;
	}

	public void setValueSchemaID(int valueschemaid) {
		this.valueschemaid = valueschemaid;
	}
	
}
