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

/**
 * The implementer of a factory needs a constructor without a property, e.g.
 * 
 * <pre>
 * public MyConnectorFactory() {
 *   super("MyConnector");
 * }
 * </pre>
 * 
 * and this class has to be added as a Java service loader, meaning in the file <br>
 * src/main/java/META-INF/services/io.rtdi.bigdata.connector.connectorframework.IConnectorFactory <br>
 * should be an single line with the class name, e.g. io.rtdi.bigdata.democonnector.MyConnectorFactory
 * 
 *
 * @param <S> ConnectionProperties
 * @param <P> ProducerProperties
 * @param <C> ConsumerProperties
 */
public abstract class ConnectorFactory <S extends ConnectionProperties, P extends ProducerProperties, C extends ConsumerProperties> implements IConnectorFactory<S, P, C> {
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
	public abstract Consumer<S,C> createConsumer(ConsumerInstanceController instance) throws IOException;

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.connectorframework.IConnectorFactory#createProducer(io.rtdi.bigdata.connector.connectorframework.controller.ProducerInstanceController)
	 */
	@Override
	public abstract Producer<S,P> createProducer(ProducerInstanceController instance) throws IOException;

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.connectorframework.IConnectorFactory#createRemoteSourceProperties(java.lang.String)
	 */
	@Override
	public abstract S createConnectionProperties(String name) throws PropertiesException;

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.connectorframework.IConnectorFactory#createConsumerProperties(java.lang.String)
	 */
	@Override
	public abstract C createConsumerProperties(String name) throws PropertiesException;
	
	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.connectorframework.IConnectorFactory#createProducerProperties(java.lang.String)
	 */
	@Override
	public abstract P createProducerProperties(String name) throws PropertiesException;

	@Override
	public String toString() {
		return connectorname;
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.connectorframework.IConnectorFactory#createBrowsingService(io.rtdi.bigdata.connector.properties.ConnectionProperties)
	 */
	@Override
	public abstract BrowsingService<S> createBrowsingService(ConnectionController controller) throws IOException;	
	
}
