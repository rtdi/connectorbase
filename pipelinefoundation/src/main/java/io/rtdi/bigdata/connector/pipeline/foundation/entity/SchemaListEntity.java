package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import java.util.List;

public class SchemaListEntity {

	private List<String> schemas;
	
	public SchemaListEntity() {
	}

	public SchemaListEntity(List<String> schemas) {
		this.schemas = schemas;
	}

	public List<String> getSchemas() {
		return schemas;
	}

	public void setSchemas(List<String> schemas) {
		this.schemas = schemas;
	}

}
