package io.rtdi.bigdata.connector.connectorframework.controller;

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
	}

	@Override
	protected void startControllerImpl() throws PropertiesException {
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
	
}
