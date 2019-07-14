package io.rtdi.bigdata.connector.pipeline.foundation;

import java.io.IOException;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.connector.pipeline.foundation.enums.RowType;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.connector.properties.ProducerProperties;

/**
 * A ProducerSession is a single connection to the topic server and used to load data into various topics in a transactional way.
 *
 */
public abstract class ProducerSession<T extends TopicHandler> {
	private String sourcetransactionidentifier = null;
	private long changetime;
	private boolean isopen = false;
	private ProducerProperties properties;
	protected Logger logger = LogManager.getLogger(this.getClass().getName());
	private String tenantid;
	private IPipelineBase<T> api;


	/**
	 * This constructor should not throw exceptions as it creates the object only. It should not start anything.
	 * 
	 * @param properties optional, needed by the producer
	 * @param tenantid used for this session
	 * @param api
	 */
	public ProducerSession(ProducerProperties properties, String tenantid, IPipelineBase<T> api) {
		super();
		this.properties = properties;
		this.tenantid = tenantid;
		this.api = api;
	}

	/**
	 * Start a new transaction and assign its metadata. This method is usually not invoked directly but via {@link PipelineAbstract#beginProducerTransaction(ProducerSession, String) this}
	 * 
	 * @param sourcetransactionid
	 * @throws PipelineRuntimeException
	 * @throws IOException
	 */
	public final void beginTransaction(String sourcetransactionid) throws PipelineRuntimeException {
		if (isopen) {
			throw new PipelineRuntimeException("Cannot begin a new transaction while it is not completed");
		}
		this.sourcetransactionidentifier = sourcetransactionid;
		this.changetime = System.currentTimeMillis();
		beginImpl();
	}

	/**
	 * @throws PipelineRuntimeException
	 * @throws IOException
	 * 
	 * @see PipelineAbstract#commitProducerTransaction(ProducerSession)
	 */
	public final void commitTransaction() throws IOException {
		commitImpl();
		isopen = false;
		sourcetransactionidentifier = null;
	}

	/**
	 * @throws PipelineRuntimeException
	 * @throws IOException
	 * 
	 * @see PipelineAbstract#abortProducerTransaction(ProducerSession)
	 */
	public final void abortTransaction() throws PipelineRuntimeException {
		isopen = false;
		sourcetransactionidentifier = null;
		abort();
	}

	/**
	 * Called at the end of {@link #beginTransaction(String)} to provide the implementer with a place to add custom code.
	 * 
	 * @throws PipelineRuntimeException
	 * @throws IOException
	 */
	public abstract void beginImpl() throws PipelineRuntimeException;

	/**
	 * Called at the end of {@link #commitTransaction()} to provide the implementer with a place to add custom code.
	 * 
	 * @throws PipelineRuntimeException
	 * @throws IOException
	 */
	public abstract void commitImpl() throws IOException;

	/**
	 * Called at the end of {@link #abortTransaction()} to provide the implementer with a place to add custom code.
	 * 
	 * @throws PipelineRuntimeException
	 * @throws IOException
	 */
	protected abstract void abort() throws PipelineRuntimeException;

	/**
	 * @return The current/last transaction's sourcetransactionidentifier as passed in the {@link #beginTransaction(String)}
	 */
	public String getSourceTransactionIdentifier() {
		return sourcetransactionidentifier;
	}

	/**
	 * @return The timestamp when the current/last invocation of {@link #beginTransaction(String)} was done. Is used as part of the record metadata.
	 */
	public long getChangetime() {
		return changetime;
	}

	/**
	 * @return True if the transaction was begun but not yet committed or aborted.
	 */
	public boolean isOpen() {
		return isopen;
	}
	
	/**
	 * @return The ProducerProperties as being passed into the constructor
	 */
	public ProducerProperties getProperties() {
		return properties;
	}
	
	/**
	 *  This method is called whenever a new connection to the topic server should be created
	 *  
	 * @throws PipelineRuntimeException
	 * @throws IOException
	 */
	public abstract void open() throws PipelineRuntimeException;
	
	/**
	 * Cleanup every connection with the server.
	 */
	public abstract void close();
	
	/**
	 * Add a new row to the ProducerSession for being sent to the topic. It does modify the provided valuerecord
	 * by filling the internal metadata columns.
	 * 
	 * @param topic The Topic this record is put into
	 * @param partition Optional partition information
	 * @param keyrecord
	 * @param valuerecord
	 * @param changetype
	 * @param sourceRowID
	 * @param sourceSystemID
	 * @throws PipelineRuntimeException
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public final void addRow(TopicHandler topic, Integer partition, SchemaHandler handler, GenericRecord keyrecord, GenericRecord valuerecord,
			RowType changetype, String sourceRowID, String sourceSystemID) throws IOException {
		if (getSourceTransactionIdentifier() != null) {
			valuerecord.put(SchemaConstants.SCHEMA_COLUMN_SOURCE_TRANSACTION, getSourceTransactionIdentifier());
		}
		valuerecord.put(SchemaConstants.SCHEMA_COLUMN_CHANGE_TIME, getChangetime());
		valuerecord.put(SchemaConstants.SCHEMA_COLUMN_CHANGE_TYPE, changetype.getIdentifer());
		valuerecord.put(SchemaConstants.SCHEMA_COLUMN_SOURCE_SYSTEM, sourceSystemID);
		if (sourceRowID != null) {
			valuerecord.put(SchemaConstants.SCHEMA_COLUMN_SOURCE_ROWID, sourceRowID);
		}
		addRowImpl((T) topic, partition, handler, keyrecord, valuerecord);
	}

	/**
	 * The actual internal implementation of how to add the record into the queue. 
	 * It is protected as it should never be called directly but via this {@link #addRow(TopicHandler, Integer, Record, Record, RowType, String, String) addRow} version.
	 * 
	 * @param transaction
	 * @param topic
	 * @param partition
	 * @param keyrecord
	 * @param valuerecord
	 * @throws PipelineRuntimeException
	 * @throws IOException
	 */
	protected abstract void addRowImpl(T topic, Integer partition, SchemaHandler handler, GenericRecord keyrecord, GenericRecord valuerecord) throws IOException;

	public abstract void addRowBinary(TopicHandler topic, Integer partition, byte[] keyrecord, byte[] valuerecord) throws IOException;

		
	@Override
	public String toString() {
		return "ProducerSession " + properties.getName();
	}

	/**
	 * @return the configured tenant for this session
	 */
	public String getTenantId() {
		return tenantid;
	}
	
	/**
	 * @return pipeline api as quick access
	 */
	public IPipelineBase<T> getPipelineAPI() {
		return api;
	}

	/**
	 * @return the transaction id or null if no transaction is active yet
	 */
	public String getTransactionID() {
		return sourcetransactionidentifier;
	}
}
