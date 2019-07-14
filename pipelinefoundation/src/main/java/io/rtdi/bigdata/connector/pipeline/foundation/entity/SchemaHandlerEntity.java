package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import javax.xml.bind.annotation.XmlTransient;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;

public class SchemaHandlerEntity {

	private String schemaname;
	private String description;
	private String keyschema;
	private String valueschema;
	private int keyschemaid;
	private int valueschemaid;

	public SchemaHandlerEntity() {
	}

	public SchemaHandlerEntity(SchemaHandler handler) {
		this.schemaname = handler.getSchemaName().getName();
		this.keyschemaid = handler.getDetails().getKeySchemaID();
		this.valueschemaid = handler.getDetails().getValueSchemaID();
		this.keyschema = handler.getKeySchema().toString();
		this.valueschema = handler.getValueSchema().toString();
	}

	public String getSchemaName() {
		return schemaname;
	}

	public void setSchemaName(String schemaname) {
		this.schemaname = schemaname;
	}

	public int getValueSchemaId() {
		return valueschemaid;
	}

	public void setValueSchemaId(int valueschemaid) {
		this.valueschemaid = valueschemaid;
	}

	public int getKeySchemaId() {
		return keyschemaid;
	}

	public void setKeySchemaId(int keyschemaid) {
		this.keyschemaid = keyschemaid;
	}

	public String getValueSchemaString() {
		return valueschema;
	}

	public void setValueSchemaString(String valueschema) {
		this.valueschema = valueschema;
	}

	public String getKeySchemaString() {
		return keyschema;
	}

	public void setKeySchemaString(String keyschema) {
		this.keyschema = keyschema;
	}

	@XmlTransient
	public Schema getKeySchema() {
        return new Schema.Parser().parse(keyschema);
	}
	
	@XmlTransient
	public Schema getValueSchema() {
        return new Schema.Parser().parse(valueschema);
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
}
