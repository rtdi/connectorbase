package io.rtdi.bigdata.connector.connectorframework.controller;

import java.io.IOException;
import java.util.HashMap;

import io.rtdi.bigdata.connector.connectorframework.IConnectorFactory;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerExitType;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;

public class ProducerController extends Controller<ProducerInstanceController> {

	private ProducerProperties producerprops;
	private ConnectionController connectioncontroller;
	private int instancecount;

	public ProducerController(ProducerProperties producerprops, ConnectionController connectioncontroller) throws PropertiesException {
		super(producerprops.getName());
		this.producerprops = producerprops;
		this.connectioncontroller = connectioncontroller;
		instancecount = producerprops.getInstanceCount();
		for (int i = 0 ; i<instancecount; i++) {
			String name = getName() + " " + String.valueOf(i);
			ProducerInstanceController instance = new ProducerInstanceController(name, this, i);
			addChild(name, instance);
		}
	}

	@Override
	protected void stopControllerImpl(ControllerExitType exittype) {
		stopChildControllers(exittype);
	}

	@Override
	protected void startControllerImpl() throws IOException {
		startChildController();
	}

	public ProducerProperties getProducerProperties() {
		return producerprops;
	}

	@Override
	protected String getControllerType() {
		return "ProducerController";
	}

	public ConnectionProperties getConnectionProperties() {
		return connectioncontroller.getConnectionProperties();
	}

	public IPipelineAPI<?, ?, ?, ?> getPipelineAPI() {
		return connectioncontroller.getPipelineAPI();
	}

	public IConnectorFactory<?, ?, ?> getConnectorFactory() {
		return connectioncontroller.getConnectorFactory();
	}

	public HashMap<String, ProducerInstanceController> getInstances() {
		return getChildControllers();
	}

	public ConnectionController getConnectionController() {
		return connectioncontroller;
	}

	public ConnectorController getConnectorController() {
		return connectioncontroller.getConnectorController();
	}

	public int getInstanceCount() {
		return instancecount;
	}

	public long getRowsProcessedCount() {
		long count = 0;
		if (getInstances() != null) {
			for (ProducerInstanceController c : getInstances().values()) {
				count += c.getRowsProduced();
			}
		}
		return count;
	}

	public Long getLastProcessed() {
		Long last = null;
		if (getInstances() != null) {
			for (ProducerInstanceController c : getInstances().values()) {
				Long l = c.getLastProcessed();
				if (l != null) {
					if (last == null || last < l) {
						last = l;
					}
				}
			}
		}
		return last;
	}

	@Override
	protected void updateLandscape() {
		if (getInstances() != null) {
			for (ProducerInstanceController c : getInstances().values()) {
				c.updateLandscape();
			}
		}
	}

	@Override
	protected void updateSchemaCache() {
		if (getInstances() != null) {
			for (ProducerInstanceController c : getInstances().values()) {
				c.updateSchemaCache();
			}
		}
	}
}
