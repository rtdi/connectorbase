package io.rtdi.bigdata.connector.connectorframework.rest.entity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaRegistryName;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import jakarta.xml.bind.annotation.XmlElement;

public class Schemas {
	private ArrayList<Schema> schemas;

	public Schemas() {
		super();
	}

	public Schemas(IPipelineAPI<?, ?, ?, ?> api) throws PipelineRuntimeException, IOException {
		List<SchemaRegistryName> entities = api.getSchemas();
		schemas = new ArrayList<Schema>(entities.size());
		for (SchemaRegistryName entity : entities) {
			Schema data = new Schema(entity.getName());
			schemas.add(data);
		}
	}

	@XmlElement
	public ArrayList<Schema> getSchemas() {
		return schemas;
	}
	
	
	public static class Schema {
		private String schemaname;

		public Schema() {
			super();
		}
		
		public Schema(String name) {
			this.schemaname = name;
		}
		
		@XmlElement
		public String getSchemaname() {
			return schemaname;
		}

	}
}
