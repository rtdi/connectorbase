package io.rtdi.bigdata.connector.connectorframework.controller;

import java.io.IOException;
import java.util.HashMap;

import io.rtdi.bigdata.connector.connectorframework.IConnectorFactory;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerExitType;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;

public class ConsumerController extends Controller<ConsumerInstanceController> {

	private ConsumerProperties consumerprops;
	private ConnectionController connectioncontroller;

	public ConsumerController(ConsumerProperties consumerprops, ConnectionController connectioncontroller) throws PropertiesException {
		super(consumerprops.getName());
		this.consumerprops = consumerprops;
		this.connectioncontroller = connectioncontroller;
		int instancecount = consumerprops.getInstances();
		for (int i=0; i<instancecount; i++) {
			String s = String.valueOf(i);
			ConsumerInstanceController instance = new ConsumerInstanceController(getName() + " " + s, this);
			addChild(s, instance);
		}
	}

	@Override
	protected void stopControllerImpl(ControllerExitType exittype) {
		super.stopChildControllers(exittype);
	}

	@Override
	protected void startControllerImpl() throws IOException {
		startChildController();
	}

	public ConsumerProperties getConsumerProperties() {
		return consumerprops;
	}

	@Override
	protected String getControllerType() {
		return "ConsumerController";
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

	public HashMap<String, ConsumerInstanceController> getInstances() {
		return getChildControllers();
	}

	public long getRowsProcessedCount() {
		long count = 0;
		if (getInstances() != null) {
			for (ConsumerInstanceController c : getInstances().values()) {
				count += c.getRowsFetched();
			}
		}
		return count;
	}

	public int getInstanceCount() {
		return consumerprops.getInstanceCount();
	}

	public Long getLastProcessed() {
		Long last = null;
		if (getInstances() != null) {
			for (ConsumerInstanceController c : getInstances().values()) {
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
			for (ConsumerInstanceController c : getInstances().values()) {
				c.updateLandscape();
			}
		}
	}

	@Override
	protected void updateSchemaCache() {
		if (getInstances() != null) {
			for (ConsumerInstanceController c : getInstances().values()) {
				c.updateSchemaCache();
			}
		}
	}
	
}
