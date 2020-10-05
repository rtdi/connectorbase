package io.rtdi.bigdata.connector.connectorframework.controller;

import java.io.IOException;

import io.rtdi.bigdata.connector.connectorframework.Consumer;
import io.rtdi.bigdata.connector.connectorframework.IConnectorFactoryConsumer;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ServiceEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerExitType;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.OperationState;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;

public class ConsumerInstanceController extends ThreadBasedController<Controller<?>> {

	private ConsumerController consumercontroller;
	private Long lastdatatimestamp = null;
	private long lastoffset = 0;
	private int fetchcalls = 0;
	private boolean updateConsumerMetadata;
	private long rowsprocessed;
	private OperationState state = OperationState.STOPPED;

	public ConsumerInstanceController(String name, ConsumerController consumercontroller) {
		super(name);
		this.consumercontroller = consumercontroller;
	}

	@Override
	protected void startThreadControllerImpl() throws PipelineRuntimeException {
	}

	@Override
	protected void stopThreadControllerImpl(ControllerExitType exittype) {
	}

	@Override
	public void runUntilError() throws IOException {
		updateConsumerMetadata = true;
		state = OperationState.OPEN;
		try (Consumer<?,?> consumer = getConnectorFactory().createConsumer(this);) {
			state = OperationState.DONEOPEN;
			try {
				long flushtime = getProperties().getFlushMaxTime();
				long maxrows = getProperties().getFlushMaxRecords();
				long timeout = System.currentTimeMillis() + flushtime;
				long rowcounter = 0;
				consumer.setTopics();
				while (isRunning()) {
					logger.debug("Fetching Data");
					state = OperationState.REQUESTDATA;
					fetchcalls++;
					rowcounter += consumer.fetchBatch();
					state = OperationState.DONEREQUESTDATA;
					if (System.currentTimeMillis() > timeout || rowcounter >= maxrows) {
						timeout = System.currentTimeMillis() + flushtime;
						rowcounter = 0;
						state = OperationState.DOEXPLICITCOMMIT;
						consumer.flushData(); // flush/commit.
						state = OperationState.DONEEXPLICITCOMMIT;
					}
					if (updateConsumerMetadata) {
						getPipelineAPI().addConsumerMetadata(
								new ConsumerEntity(
										consumercontroller.getName(),
										consumercontroller.getConnectionProperties().getName(),
										this.getPipelineAPI(),
										consumer.getTopics()));
						String bs = getPipelineAPI().getBackingServerConnectionLabel();
						if (bs != null) {
							getPipelineAPI().addServiceMetadata(
									new ServiceEntity(
											bs,
											bs,
											getPipelineAPI().getConnectionLabel(),
											null,
											null));
						}
						updateConsumerMetadata = false;
					}
				}
			} finally {
				state = OperationState.STOPPED;
				/*
				 * Clear the interrupt flag to ensure the close() of the try operation can close all resources gracefully
				 */
				Thread.interrupted();
			}
		}
	}
	
	private ConsumerProperties getProperties() {
		return consumercontroller.getConsumerProperties();
	}
	
	@Override
	protected String getControllerType() {
		return "ConsumerInstanceController";
	}

	public ConsumerProperties getConsumerProperties() {
		return consumercontroller.getConsumerProperties();
	}

	public ConnectionProperties getConnectionProperties() {
		return consumercontroller.getConnectionProperties();
	}

	public IPipelineAPI<?, ?, ?, ?> getPipelineAPI() {
		return consumercontroller.getPipelineAPI();
	}

	public IConnectorFactoryConsumer<?,?> getConnectorFactory() {
		return (IConnectorFactoryConsumer<?, ?>) consumercontroller.getConnectorFactory();
	}

	public long getRowsFetched() {
		return rowsprocessed;
	}
	
	public Long getLastOffset() {
		return lastoffset;
	}
	
	public int getFetchCalls() {
		return fetchcalls;
	}

	public Long getLastProcessed() {
		return lastdatatimestamp;
	}

	@Override
	protected void updateLandscape() {
		this.updateConsumerMetadata = true;
	}

	@Override
	protected void updateSchemaCache() {
	}

	public void incrementRowProcessed(long offset) {
		rowsprocessed++;
		lastoffset = offset;
		lastdatatimestamp = System.currentTimeMillis();
	}

	public OperationState getOperationState() {
		return state;
	}

	public void setOperationState(OperationState state) {
		this.state = state;
	}

}
