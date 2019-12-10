package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import javax.xml.bind.annotation.XmlTransient;

import org.apache.avro.Schema;

public class SchemaEntity {

	private String schemastring;

	public SchemaEntity() {
	}

	public SchemaEntity(Schema schema) {
		this.schemastring = schema.toString();
	}

	public String getSchemaString() {
		return schemastring;
	}

	public void setSchemaString(String schemastring) {
		this.schemastring = schemastring;
	}
	
	@XmlTransient
	public Schema getSchema() {
        return new Schema.Parser().parse(schemastring);
	}
}
