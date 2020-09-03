package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ServiceProperties;
import io.rtdi.bigdata.connector.properties.atomic.PropertyRoot;

public class ServiceConfigEntity {

	public static class ServiceStep {
		private String stepname;

		public ServiceStep() {
		}
	
		public ServiceStep(File stepdir) {
			stepname = stepdir.getName();
		}
	
		public String getStepname() {
			return stepname;
		}
	
		public void setStepname(String stepname) {
			this.stepname = stepname;
		}

	}

	private PropertyRoot serviceproperties;
	private List<ServiceConfigEntity.ServiceStep> steps;
	
	public ServiceConfigEntity(ServiceProperties serviceprops, File dir) throws PropertiesException {
		serviceproperties = serviceprops.getPropertyGroupNoPasswords();
		steps = new ArrayList<>();
		for (File schemadir : dir.listFiles()) {
			if (schemadir.isDirectory()) {
				steps.add(new ServiceConfigEntity.ServiceStep(schemadir));
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

	public List<ServiceConfigEntity.ServiceStep> getSteps() {
		return steps;
	}
	
	public void setSchemas(List<ServiceConfigEntity.ServiceStep> steps) {
		this.steps = steps;
	}
}