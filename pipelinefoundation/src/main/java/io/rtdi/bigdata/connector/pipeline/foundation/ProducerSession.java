package io.rtdi.bigdata.connector.pipeline.foundation;

import java.io.IOException;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RowType;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.connector.properties.ProducerProperties;

/**
 * A ProducerSession is a single connection to the topic server and used to load data into various topics in a transactional way.
 * @param <T> TopicHandler
 *
 */
public abstract class ProducerSession<T extends TopicHandler> {
	private String sourcetransactionidentifier = null;
	private long changetime;
	private boolean isopen = false;
	private ProducerProperties properties;
	protected Logger logger = LogManager.getLogger(this.getClass().getName());
	private IPipelineBase<?, T> api;


	/**
	 * This constructor should not throw exceptions as it creates the object only. It should not start anything.
	 * 
	 * @param properties optional, needed by the producer
	 * @param api to use
	 */
	public ProducerSession(ProducerProperties properties, IPipelineBase<?, T> api) {
		super();
		this.properties = properties;
		this.api = api;
	}

	/**
	 * Start a new transaction and assign its metadata.
	 * 
	 * @param sourcetransactionid a strictly ascending id. Will be used to find a recovery point in case of an error.
	 * @throws PipelineRuntimeException in case the previous transaction is open still
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
	 * Commit the transaction in the producer, thus telling the source this record had been received and cannot get lost anymore.
	 * 
	 * @throws IOException in case anything goes wrong during the commit
	 * 
	 */
	public final void commitTransaction() throws IOException {
		commitImpl();
		isopen = false;
		sourcetransactionidentifier = null;
	}

	/**
	 * Rollback the current transaction.
	 * 
	 * @throws PipelineRuntimeException in case the rollback failed
	 * 
	 */
	public final void abortTransaction() throws PipelineRuntimeException {
		isopen = false;
		sourcetransactionidentifier = null;
		abort();
	}

	/**
	 * Called at the end of {@link #beginTransaction(String)} to provide the implementer with a place to add custom code.
	 * Normally not called directly.
	 * 
	 * @throws PipelineRuntimeException in case anything goes wrong
	 */
	public abstract void beginImpl() throws PipelineRuntimeException;

	/**
	 * Called at the end of {@link #commitTransaction()} to provide the implementer with a place to add custom code.
	 * Normally not called directly.
	 * 
	 * @throws IOException in case anything goes wrong
	 */
	public abstract void commitImpl() throws IOException;

	/**
	 * Called at the end of {@link #abortTransaction()} to provide the implementer with a place to add custom code.
	 * 
	 * @throws PipelineRuntimeException if the abort fails
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
	 * @throws PipelineRuntimeException in case anything goes wrong
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
	 * @param topic this record should be put into
	 * @param partition optional partition information
	 * @param handler SchemaHandler with the latest schema IDs
	 * @param keyrecord Avro record of the key
	 * @param valuerecord Avro record with the payload
	 * @param changetype indicator how the record should be processed, e.g. inserted, deleted etc
	 * @param sourceRowID optional information how to identify the record in the source
	 * @param sourceSystemID optional information about the source system this record is produced from
	 * @throws IOException in case anything goes wrong
	 */
	@SuppressWarnings("unchecked")
	public final void addRow(TopicHandler topic, Integer partition, SchemaHandler handler, JexlRecord keyrecord, JexlRecord valuerecord,
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
	 * Same as {@link #addRow(TopicHandler, Integer, SchemaHandler, JexlRecord, JexlRecord, RowType, String, String)} but creates
	 * the keyrecord by copying the corresponding values from the valuerecord. 
	 * 
	 * @param topic this record should be put into
	 * @param partition optional partition information
	 * @param handler SchemaHandler with the latest schema IDs
	 * @param valuerecord Avro record with the payload
	 * @param changetype indicator how the record should be processed, e.g. inserted, deleted etc
	 * @param sourceRowID optional information how to identify the record in the source
	 * @param sourceSystemID optional information about the source system this record is produced from
	 * @throws IOException in case anything goes wrong
	 */
	public final void addRow(TopicHandler topic, Integer partition, SchemaHandler handler, JexlRecord valuerecord,
			RowType changetype, String sourceRowID, String sourceSystemID) throws IOException {
		JexlRecord keyrecord = new JexlRecord(handler.getKeySchema());
		for (Field f : keyrecord.getSchema().getFields()) {
			keyrecord.put(f.name(), valuerecord.get(f.name()));
		}
		addRow(topic, partition, handler, keyrecord, valuerecord, changetype, sourceSystemID, sourceSystemID);
	}


	/**
	 * The actual internal implementation of how to add the record into the queue. 
	 * It is protected as it should never be called directly but via this {@link #addRow(TopicHandler, Integer, SchemaHandler, GenericRecord, GenericRecord, RowType, String, String) addRow} version.
	 * 
	 * @param topic this record should be put into
	 * @param partition optional partition information
	 * @param handler SchemaHandler with the latest schema IDs
	 * @param keyrecord Avro record of the key
	 * @param valuerecord Avro record of the value
	 * @throws IOException in case anything goes wrong
	 */
	protected abstract void addRowImpl(T topic, Integer partition, SchemaHandler handler, JexlRecord keyrecord, JexlRecord valuerecord) throws IOException;

	/**
	 * Some implementations, the http-pipeline with the http-server, exchange the data in the serialized format already. Then this version comes in handy.
	 * Therefore the addRowImpl(TopicHandler, Integer, SchemaHandler, GenericRecord, GenericRecord)  
	 * 
	 * @param topic this record should be put into
	 * @param partition optional partition information
	 * @param keyrecord Avro record of the key in serialized form
	 * @param valuerecord Avro record with the payload in serialized form
	 * @throws IOException in case anything goes wrong
	 */
	public abstract void addRowBinary(TopicHandler topic, Integer partition, byte[] keyrecord, byte[] valuerecord) throws IOException;

		
	@Override
	public String toString() {
		return "ProducerSession " + properties.getName();
	}

	/**
	 * @return pipeline api as quick access
	 */
	public IPipelineBase<?, T> getPipelineAPI() {
		return api;
	}

	/**
	 * @return the transaction id or null if no transaction is active yet
	 */
	public String getTransactionID() {
		return sourcetransactionidentifier;
	}
}
