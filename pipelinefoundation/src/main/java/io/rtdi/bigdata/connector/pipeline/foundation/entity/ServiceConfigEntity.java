package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ServiceProperties;
import io.rtdi.bigdata.connector.properties.atomic.PropertyRoot;

public class ServiceConfigEntity {

	public static class ServiceSchema {
		private String schemaname;
		private List<ServiceConfigEntity.ServiceStep> steps;
	
		public ServiceSchema() {
		}
	
		public ServiceSchema(File schemadir) {
			if (schemadir != null) {
				steps = new ArrayList<>();
				schemaname = schemadir.getName();
				for (File stepdir : schemadir.listFiles()) {
					if (stepdir.isDirectory()) {
						steps.add(new ServiceConfigEntity.ServiceStep(stepdir));
					}
				}
			}
		}
	
		public String getSchemaname() {
			return schemaname;
		}
	
		public List<ServiceConfigEntity.ServiceStep> getSteps() {
			return steps;
		}
	
		public void setSteps(List<ServiceConfigEntity.ServiceStep> steps) {
			this.steps = steps;
		}
	
		public void setSchemaname(String schemaname) {
			this.schemaname = schemaname;
		}
	
	}

	public static class ServiceStep {
		private String stepname;
		private String path;
	
		public ServiceStep() {
		}
	
		public ServiceStep(File stepdir) {
			stepname = stepdir.getName();
			File parent = stepdir.getParentFile();
			path = parent.getName() + "/" + stepdir.getName();
		}
	
		public String getStepname() {
			return stepname;
		}
	
		public void setStepname(String stepname) {
			this.stepname = stepname;
		}
	
		public String getPath() {
			return path;
		}
	
		public void setPath(String path) {
			this.path = path;
		}
	}

	private PropertyRoot serviceproperties;
	private List<ServiceConfigEntity.ServiceSchema> schemas;
	
	public ServiceConfigEntity(ServiceProperties serviceprops, File dir) throws PropertiesException {
		serviceproperties = serviceprops.getPropertyGroupNoPasswords();
		schemas = new ArrayList<>();
		for (File schemadir : dir.listFiles()) {
			if (schemadir.isDirectory()) {
				schemas.add(new ServiceConfigEntity.ServiceSchema(schemadir));
			}
		}
	}

	public ServiceConfigEntity() {
		super();
	}

	public ServiceConfigEntity(PropertyRoot props) {
		this();
		serviceproperties = props;
	}

	public PropertyRoot getServiceproperties() {
		return serviceproperties;
	}

	public void setServiceproperties(PropertyRoot serviceproperties) {
		this.serviceproperties = serviceproperties;
	}

	public List<ServiceConfigEntity.ServiceSchema> getSchemas() {
		return schemas;
	}
	
	public void setSchemas(List<ServiceConfigEntity.ServiceSchema> schemas) {
		this.schemas = schemas;
	}
}