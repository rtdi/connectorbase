package io.rtdi.bigdata.connector.connectorframework;

import java.io.IOException;

import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;

/**
 *
 * @param <S> ConnectionProperties
 */
public interface IConnectorFactory<S extends ConnectionProperties> {

	/**
	 * The name of the connector.
	 * 
	 * @return The name of the connector
	 */
	String getConnectorName();

	/**
	 * 
	 * @param name Unique name of the connection
	 * @return ConnectionProperties of this connector with all parameters needed to connect to the system, all values default
	 * @throws PropertiesException if properties are invalid
	 */
	S createConnectionProperties(String name) throws PropertiesException;

	/**
	 * Create a BrowsingService to read metadata from the source system.
	 * 
	 * @param controller ConnectionController of the connection
	 * @return BrowsingService instance
	 * @throws IOException if network error
	 */
	BrowsingService<S> createBrowsingService(ConnectionController controller) throws IOException;

	boolean supportsBrowsing();

}