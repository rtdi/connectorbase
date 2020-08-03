package io.rtdi.bigdata.connector.connectorframework;

import java.io.IOException;

import io.rtdi.bigdata.connector.connectorframework.controller.ServiceController;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ServiceProperties;

/**
 *
 */
public interface IConnectorFactoryService {

	Service createService(ServiceController instance) throws IOException;

	ServiceProperties createServiceProperties(String servicename) throws PropertiesException;
	
}