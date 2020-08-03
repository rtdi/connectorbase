package io.rtdi.bigdata.connector.connectorframework;

import java.io.IOException;

import io.rtdi.bigdata.connector.connectorframework.controller.ConsumerInstanceController;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;

/**
 *
 * @param <S> ConnectionProperties
 * @param <C> ConsumerProperties
 */
public interface IConnectorFactoryConsumer<S extends ConnectionProperties, C extends ConsumerProperties> extends IConnectorFactory<S> {

	/**
	 * 
	 * @param instance ConsumerInstanceController
	 * @return the concrete consumer class for this connector to be executed by the controller
	 * @throws IOException if network error
	 */
	Consumer<S, C> createConsumer(ConsumerInstanceController instance) throws IOException;

	/**
	 * 
	 * @param name Unique name of the consumer
	 * @return ConsumerProperties of this connector with all parameters needed to connect to the system, all values default
	 * @throws PropertiesException if properties are invalid
	 */
	C createConsumerProperties(String name) throws PropertiesException;

}