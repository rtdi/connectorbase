package io.rtdi.bigdata.connector.connectorframework;

import java.io.IOException;

import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConsumerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.controller.ProducerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.controller.ServiceController;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;
import io.rtdi.bigdata.connector.properties.ServiceProperties;

/**
 *
 * @param <S> ConnectionProperties
 * @param <P> ProducerProperties
 * @param <C> ConsumerProperties
 */
public interface IConnectorFactory<S extends ConnectionProperties, P extends ProducerProperties, C extends ConsumerProperties> {

	/**
	 * The name of the connector.
	 * 
	 * @return The name of the connector
	 */
	String getConnectorName();

	/**
	 * 
	 * @param instance ConsumerInstanceController
	 * @return the concrete consumer class for this connector to be executed by the controller
	 * @throws IOException if network error
	 */
	Consumer<S, C> createConsumer(ConsumerInstanceController instance) throws IOException;

	/**
	 * @param instance ProducerInstanceController
	 * @return the concrete producer class for this connector to be executed by the controller
	 * @throws IOException if network error
	 */
	Producer<S, P> createProducer(ProducerInstanceController instance) throws IOException;

	/**
	 * 
	 * @param name Unique name of the connection
	 * @return ConnectionProperties of this connector with all parameters needed to connect to the system, all values default
	 * @throws PropertiesException if properties are invalid
	 */
	S createConnectionProperties(String name) throws PropertiesException;

	/**
	 * 
	 * @param name Unique name of the consumer
	 * @return ConsumerProperties of this connector with all parameters needed to connect to the system, all values default
	 * @throws PropertiesException if properties are invalid
	 */
	C createConsumerProperties(String name) throws PropertiesException;

	/**
	 * 
	 * @param name Unique name of the producer
	 * @return ProducerProperties of this connector with all parameters needed to connect to the system, all values default
	 * @throws PropertiesException if properties are invalid
	 */
	P createProducerProperties(String name) throws PropertiesException;

	/**
	 * Create a BrowsingService to read metadata from the source system.
	 * 
	 * @param controller ConnectionController of the connection
	 * @return BrowsingService instance
	 * @throws IOException if network error
	 */
	BrowsingService<S> createBrowsingService(ConnectionController controller) throws IOException;

	Service createService(ServiceController instance) throws IOException;

	ServiceProperties<?> createServiceProperties(String servicename) throws PropertiesException;
	
	boolean supportsConnections();
	
	boolean supportsServices();
	
	boolean supportsProducers();
	
	boolean supportsConsumers();
	
	boolean supportsBrowsing();

}