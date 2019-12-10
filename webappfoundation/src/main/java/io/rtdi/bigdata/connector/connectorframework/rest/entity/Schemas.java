package io.rtdi.bigdata.connector.connectorframework.rest.entity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;

import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;

public class Schemas {
	private ArrayList<Schema> schemas;

	public Schemas() {
		super();
	}

	public Schemas(IPipelineAPI<?, ?, ?, ?> api) throws PipelineRuntimeException, IOException {
		List<String> entities = api.getSchemas();
		schemas = new ArrayList<Schema>(entities.size());
		for (String entity : entities) {
			Schema data = new Schema(entity);
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
