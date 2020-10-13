package io.rtdi.bigdata.connector.connectorframework;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.connector.connectorframework.controller.ConsumerInstanceController;
import io.rtdi.bigdata.connector.pipeline.foundation.ConsumerSession;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.IProcessFetchedRow;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerState;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.OperationState;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;

/**
 * A connector implements the consumer class to read data from the PipelineAPI and put it into a target system.
 * The code sequence is
 * <ul><li>constructor: does establish the connection with the PipelineAPI server</li>
 * <li>fetchBatch() of the PipelineAPI calls for each row 
 * {@link #process(TopicName, long, long, int, JexlRecord, JexlRecord)}</li>
 * <li>flushDataImpl() is called once a while to commit the data</li>
 * <li>close is called</li>
 * </ul>
 * 
 * @param <S> ConnectionProperties
 * @param <C> ConsumerProperties
 */
public abstract class Consumer<S extends ConnectionProperties, C extends ConsumerProperties> implements Closeable, IProcessFetchedRow {

	private ConsumerSession<?> consumersession;
	private ConsumerInstanceController instance;
	protected final Logger logger;

	/**
	 * Create a new consumer session to read data from the PipelineAPI
	 * 
	 * @param instance ConsumerInstanceController running this Consumer
	 * @throws IOException if open fails
	 */
	public Consumer(ConsumerInstanceController instance) throws IOException {
		super();
		consumersession = instance.getPipelineAPI().createNewConsumerSession(instance.getConsumerProperties());
		consumersession.setController(instance);
		consumersession.open();
		this.instance = instance;
		logger = LogManager.getLogger(this.getClass().getName());
	}

	@Override
	public void close() {
		consumersession.close();
		closeImpl();
	}
	
	@Override
	public boolean isActive() {
		return instance.isRunning();
	}

	/**
	 * Place for the Consumer to close all resources after the consumer session is closed.
	 */
	protected abstract void closeImpl();

	/**
	 * This method is called by the controller in a loop and is supposed to process a set of records.
	 * The size of the batch is controlled by the consumer session properties flushms and max_records.
	 * 
	 * @see #fetchBatchStart()
	 * @see #fetchBatchEnd()
	 * 
	 * @return # of rows fetched by the consumer session
	 * @throws IOException if network error
	 */
	public final int fetchBatch() throws IOException {
		fetchBatchStart();
		int rowcount = consumersession.fetchBatch(this);
		fetchBatchEnd();
		return rowcount;
	}
	
	public void setOperationState(OperationState state) {
		instance.setOperationState(state);
	}

	public final void flushData() throws IOException {
		flushDataImpl();
		consumersession.commit();
	}
	
	/**
	 * Allows the consumer to do something at the beginning of the batch, e.g. create an empty array to hold the batch of records being read.
	 * The individual rows are then added in either of the two process() methods.
	 * 
	 * @throws IOException if network error
	 * 
	 * @see #process(TopicName, long, long, int, JexlRecord, JexlRecord)
	 */
	public abstract void fetchBatchStart() throws IOException;
	
	/**
	 * Called at the end of a batch and allows the consumer to e.g. write all collected records since {@link #fetchBatchStart()}.
	 * 
	 * @throws IOException if network error
	 */
	public abstract void fetchBatchEnd() throws IOException;
	
	/**
	 * When the controller asks to flush/commit the data, this method is called.<br>
	 * Note that this is not the same as {@link #fetchBatchEnd()}. A Pipeline might support transactional consistency and then 
	 * 100s of fetchBatch calls together should be committed into the database once.
	 * 
	 * @throws IOException if network error
	 */
	public abstract void flushDataImpl() throws IOException;

	@SuppressWarnings("unchecked")
	public S getConnectionProperties() {
		return (S) instance.getConnectionProperties();
	}

	/**
	 * Convenience function to return the instance controller's PipelineAPI
	 * 
	 * @return instance controller's PipelineAPI
	 */
	public IPipelineAPI<?,?,?,?> getPipelineAPI() {
		return instance.getPipelineAPI();
	}

	/**
	 * @return consumersession used to read from the PipelineAPI
	 */
	public ConsumerSession<?> getConsumerSession() {
		return consumersession;
	}

	/**
	 * @return topics the consumer session reads data from
	 */
	public Map<String, ? extends TopicHandler> getTopics() {
		return consumersession.getTopics();
	}
	
	/**
	 * Convenience function for the controller
	 * 
	 * @return System.timeInMillis() of when the consumer session did update the metadata last time
	 */
	public long getLastMetadataChange() {
		return consumersession.getLastMetadataChange();
	}
	
	/**
	 * Asks the consumersession to set all topics it reads from. 
	 * Used by the controller.
	 * 
	 * @throws PropertiesException if topics cannot be set
	 */
	public void setTopics() throws PropertiesException {
		consumersession.setTopics();
	}
	
	@Override
	public String toString() {
		return "Consumer " + instance.getName();
	}

	/**
	 * @return consumer session's last offset information for monitoring purposes
	 */
	public long getLastOffset() {
		return consumersession.getLastOffset();
	}

	/**
	 * @return instance controller's status for monitoring purposes
	 */
	public ControllerState getControllerState() {
		return instance.getState();
	}

	@Override
	public void incrementRowsProcessed(long offset, long offsettimestamp) {
		instance.incrementRowProcessed(offset, offsettimestamp);
	}
}
