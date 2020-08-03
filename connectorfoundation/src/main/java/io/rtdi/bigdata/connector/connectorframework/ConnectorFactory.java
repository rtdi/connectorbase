package io.rtdi.bigdata.connector.connectorframework;

import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.LogicalDataTypesRegistry;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;

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
 */
public abstract class ConnectorFactory <S extends ConnectionProperties> implements IConnectorFactory<S> {
	private String connectorname;

	public ConnectorFactory(String connectorname) {
		this.connectorname = connectorname;
		LogicalDataTypesRegistry.registerAll();
	}

	@Override
	public String getConnectorName() {
		return connectorname;
	}

	@Override
	public String toString() {
		return connectorname;
	}
	
}
