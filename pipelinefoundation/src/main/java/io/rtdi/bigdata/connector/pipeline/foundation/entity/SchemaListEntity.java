package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import java.util.ArrayList;
import java.util.List;

import io.rtdi.bigdata.connector.pipeline.foundation.SchemaRegistryName;

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
	
	public List<SchemaRegistryName> schemasAsRegistryName() {
		List<SchemaRegistryName> r = new ArrayList<>();
		if (schemas != null) {
			for (String schema : schemas) {
				r.add(SchemaRegistryName.createViaEncoded(schema));
			}
		}
		return r;
	}
	
	public void setSchemas(List<String> schemas) {
		this.schemas = schemas;
	}

}
