package io.sap.bigdata.connectors.pipeline.kafkadirect.schemaentity;

public class SchemaIdResponse {

	private String schemastring;
	
	public SchemaIdResponse() {
	}

	public String getSchema() {
		return schemastring;
	}

	public void setSchema(String schema) {
		this.schemastring = schema;
	}

}
