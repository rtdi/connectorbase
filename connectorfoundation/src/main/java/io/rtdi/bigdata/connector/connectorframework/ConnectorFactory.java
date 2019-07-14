package io.rtdi.bigdata.connector.connectorframework;

import java.io.IOException;

import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConsumerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.controller.ProducerInstanceController;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.LogicalDataTypesRegistry;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;

public abstract class ConnectorFactory <U extends ConnectionProperties, V extends ProducerProperties, W extends ConsumerProperties> implements IConnectorFactory<U, V, W> {
	private String connectorname;

	public ConnectorFactory(String connectorname) {
		this.connectorname = connectorname;
		LogicalDataTypesRegistry.registerAll();
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.connectorframework.IConnectorFactory#getConnectorName()
	 */
	@Override
	public String getConnectorName() {
		return connectorname;
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.connectorframework.IConnectorFactory#createConsumer(io.rtdi.bigdata.connector.connectorframework.controller.ConsumerInstanceController)
	 */
	@Override
	public abstract Consumer<U,W> createConsumer(ConsumerInstanceController instance) throws IOException;

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.connectorframework.IConnectorFactory#createProducer(io.rtdi.bigdata.connector.connectorframework.controller.ProducerInstanceController)
	 */
	@Override
	public abstract Producer<U,V> createProducer(ProducerInstanceController instance) throws IOException;

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.connectorframework.IConnectorFactory#createRemoteSourceProperties(java.lang.String)
	 */
	@Override
	public abstract U createConnectionProperties(String name) throws PropertiesException;

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.connectorframework.IConnectorFactory#createConsumerProperties(java.lang.String)
	 */
	@Override
	public abstract W createConsumerProperties(String name) throws PropertiesException;
	
	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.connectorframework.IConnectorFactory#createProducerProperties(java.lang.String)
	 */
	@Override
	public abstract V createProducerProperties(String name) throws PropertiesException;

	@Override
	public String toString() {
		return connectorname;
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.connectorframework.IConnectorFactory#createBrowsingService(io.rtdi.bigdata.connector.properties.ConnectionProperties)
	 */
	@Override
	public abstract BrowsingService<U> createBrowsingService(ConnectionController controller) throws IOException;

}
