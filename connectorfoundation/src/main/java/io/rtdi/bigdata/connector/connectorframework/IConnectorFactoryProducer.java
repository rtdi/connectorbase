package io.rtdi.bigdata.connector.connectorframework;

import java.io.IOException;

import io.rtdi.bigdata.connector.connectorframework.controller.ProducerInstanceController;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;

/**
 *
 * @param <S> ConnectionProperties
 * @param <P> ProducerProperties
 */
public interface IConnectorFactoryProducer<S extends ConnectionProperties, P extends ProducerProperties> extends IConnectorFactory<S> {

	/**
	 * @param instance ProducerInstanceController
	 * @return the concrete producer class for this connector to be executed by the controller
	 * @throws IOException if network error
	 */
	Producer<S, P> createProducer(ProducerInstanceController instance) throws IOException;

	/**
	 * 
	 * @param name Unique name of the producer
	 * @return ProducerProperties of this connector with all parameters needed to connect to the system, all values default
	 * @throws PropertiesException if properties are invalid
	 */
	P createProducerProperties(String name) throws PropertiesException;

}