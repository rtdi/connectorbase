package io.rtdi.bigdata.connector.connectorframework;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import io.rtdi.bigdata.connector.connectorframework.controller.ConsumerInstanceController;
import io.rtdi.bigdata.connector.pipeline.foundation.ConsumerSession;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.IProcessFetchedRow;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerState;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;

/**
 * A connector implements the consumer class to read data from the PipelineAPI and put it into a target system.
 * The code sequence is
 * <ul><li>constructor: does establish the connection with the PipelineAPI server</li>
 * <li>fetchBatch() of the PipelineAPI calls for each row {@link #process(String, long, int, byte[], byte[])} or 
 * {@link #process(String, long, int, org.apache.avro.generic.GenericRecord, org.apache.avro.generic.GenericRecord, int, int)}</li>
 * <li>flushDataImpl() is called once a while to commit the data</li>
 * <li>close with {@link #closeImpl()} is called to stop</li>
 * </ul>
 * 
 * @param <S> ConnectionProperties
 * @param <C> ConsumerProperties
 */
public abstract class Consumer<S extends ConnectionProperties, C extends ConsumerProperties> implements Closeable, IProcessFetchedRow {

	private ConsumerSession<?> consumersession;
	private ConsumerInstanceController instance;

	/**
	 * Create a new consumer session to read data from the PipelineAPI
	 * 
	 * @param instance ConsumerInstanceController running this Consumer
	 * @throws IOException
	 */
	public Consumer(ConsumerInstanceController instance) throws IOException {
		super();
		consumersession = instance.getPipelineAPI().createNewConsumerSession(instance.getConsumerProperties());
		consumersession.setController(instance);
		consumersession.open();
		this.instance = instance;
	}

	@Override
	public void close() {
		consumersession.close();
		closeImpl();
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
	 * @throws IOException
	 */
	public final int fetchBatch() throws IOException {
		fetchBatchStart();
		int rowcount = consumersession.fetchBatch(this);
		fetchBatchEnd();
		return rowcount;
	}

	public final void flushData() throws IOException {
		flushDataImpl();
		consumersession.commit();
	}
	
	/**
	 * Allows the consumer to do something at the beginning of the batch, e.g. create an empty array to hold the batch of records being read.
	 * The individual rows are then added in either of the two process() methods.
	 * 
	 * @throws IOException
	 * 
	 * @see #process(String, long, int, byte[], byte[])
	 * @see #process(String, long, int, org.apache.avro.generic.GenericRecord, org.apache.avro.generic.GenericRecord, int, int)
	 */
	public abstract void fetchBatchStart() throws IOException;
	
	/**
	 * Called at the end of a batch and allows the consumer to e.g. write all collected records since {@link #fetchBatchStart()}.
	 * 
	 * @throws IOException
	 */
	public abstract void fetchBatchEnd() throws IOException;
	
	/**
	 * When the controller asks to flush/commit the data, this method is called.<br/>
	 * Note that this is not the same as {@link #fetchBatchEnd()}. A Pipeline might support transactional consistency and then 
	 * 100s of fetchBatch calls together should be committed into the database once.
	 * 
	 * @throws IOException
	 */
	public abstract void flushDataImpl() throws IOException;

	public ConnectionProperties getConnectionProperties() {
		return instance.getConnectionProperties();
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
	 * @throws PropertiesException
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

}
