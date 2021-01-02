package io.rtdi.bigdata.connector.connectorframework;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.connector.connectorframework.controller.ServiceController;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.ServiceSession;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.OperationLogContainer.StateDisplayEntry;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public abstract class Service {

	private ServiceController controller;
	private ServiceSession servicesession;
	protected final Logger logger;

	public Service(ServiceController controller) throws PropertiesException {
		this.controller = controller;
		IPipelineAPI<?, ?, ?, ?> api = controller.getPipelineAPI();
		servicesession = api.createNewServiceSession(controller.getServiceProperties());
		logger = LogManager.getLogger(this.getClass().getName());
	}
	
	public void start() throws IOException {
		servicesession.start();
	}
	
	public void stop() {
		servicesession.stop();
	}

	public long getRowsProcessed() {
		return servicesession.getRowsProcessed();
	}

	public Long getLastProcessed() {
		return servicesession.getLastRowProcessed();
	}

	public Map<String, List<StateDisplayEntry>> getMicroserviceOperationLogs() {
		return servicesession.getMicroserviceOperationLogs();
	}
	
	public ServiceController getController() {
		return controller;
	}

	public abstract void updateLandscape();

	public abstract void updateSchemaCache();

}
