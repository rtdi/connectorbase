package io.rtdi.bigdata.connector.connectorframework;

import java.io.IOException;

import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConsumerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.controller.ProducerInstanceController;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;

public interface IConnectorFactory<U extends ConnectionProperties, V extends ProducerProperties, W extends ConsumerProperties> {

	/**
	 * The name of the connector.
	 * 
	 * @return The name of the connector
	 */
	String getConnectorName();

	/**
	 * 
	 * @param instance
	 * @return the concrete consumer class for this connector to be executed by the controller
	 * @throws IOException
	 */
	Consumer<U, W> createConsumer(ConsumerInstanceController instance) throws IOException;

	/**
	 * @param instance
	 * @return the concrete producer class for this connector to be executed by the controller
	 * @throws IOException
	 */
	Producer<U, V> createProducer(ProducerInstanceController instance) throws IOException;

	/**
	 * 
	 * @param name Unique name of the connection
	 * @return ConnectionProperties of this connector with all parameters needed to connect to the system, all values default
	 * @throws PropertiesException
	 */
	U createConnectionProperties(String name) throws PropertiesException;

	/**
	 * 
	 * @param name Unique name of the consumer
	 * @return ConsumerProperties of this connector with all parameters needed to connect to the system, all values default
	 * @throws PropertiesException
	 */
	W createConsumerProperties(String name) throws PropertiesException;

	/**
	 * 
	 * @param name Unique name of the producer
	 * @return ProducerProperties of this connector with all parameters needed to connect to the system, all values default
	 * @throws PropertiesException
	 */
	V createProducerProperties(String name) throws PropertiesException;

	/**
	 * Create a BrowsingService to read metadata from the source system.
	 * 
	 * @param controller ConnectionController of the connection
	 * @return BrowsingService instance
	 * @throws IOException
	 */
	BrowsingService<U> createBrowsingService(ConnectionController controller) throws IOException;

}