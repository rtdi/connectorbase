package io.rtdi.bigdata.connector.pipeline.foundation;

import java.io.IOException;

import org.apache.avro.Schema.Field;
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
	private String initialloadschema;
	private int instanceno;


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
	 * @param instancenumber producer instance number
	 * @throws PipelineRuntimeException in case the previous transaction is open still
	 */
	public final void beginDeltaTransaction(String sourcetransactionid, int instancenumber) throws PipelineRuntimeException {
		if (isopen) {
			throw new PipelineRuntimeException("Cannot begin a new transaction while it is not completed");
		}
		this.sourcetransactionidentifier = sourcetransactionid;
		this.changetime = System.currentTimeMillis();
		beginImpl();
		this.instanceno = instancenumber;
	}

	/**
	 * Start a new transaction and assign its metadata.
	 * 
	 * @param sourcetransactionid a strictly ascending id. Will be used to find a recovery point in case of an error.
	 * @param schemaname name of the table to be loaded
	 * @param instancenumber producer instance number
	 * @throws IOException 
	 */
	public final void beginInitialLoadTransaction(String sourcetransactionid, String schemaname, int instancenumber) throws IOException {
		if (isopen) {
			throw new PipelineRuntimeException("Cannot begin a new transaction while it is not completed");
		}
		this.sourcetransactionidentifier = sourcetransactionid;
		this.changetime = System.currentTimeMillis();
		beginImpl();
		markInitialLoadStart(schemaname, instancenumber);
		this.initialloadschema = schemaname;
		this.instanceno = instancenumber;
	}

	/**
	 * Commit the transaction in the producer, thus telling the source this record had been received and cannot get lost anymore.
	 * 
	 * @throws IOException in case anything goes wrong during the commit
	 * 
	 */
	public final void commitDeltaTransaction() throws IOException {
		confirmDeltaLoad(this.instanceno);
		commitImpl();
		isopen = false;
		sourcetransactionidentifier = null;
	}

	/**
	 * Commit the transaction in the producer, thus telling the source this record had been received and cannot get lost anymore.
	 * @param rowcount number of rows loaded
	 * 
	 * @throws IOException in case anything goes wrong during the commit
	 * 
	 */
	public final void commitInitialLoadTransaction(long rowcount) throws IOException {
		confirmInitialLoad(this.initialloadschema, this.instanceno, rowcount);
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
	 * Called at the end of all begin transaction methods to provide the implementer with a place to add custom code.
	 * Normally not called directly.
	 * 
	 * @throws PipelineRuntimeException in case anything goes wrong
	 */
	public abstract void beginImpl() throws PipelineRuntimeException;

	/**
	 * Called at the end of commits to provide the implementer with a place to add custom code.
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
	 * @return The current/last transaction's sourcetransactionidentifier as passed in the begin transaction
	 */
	public String getSourceTransactionIdentifier() {
		return sourcetransactionidentifier;
	}

	/**
	 * @return The timestamp when the current/last invocation of begin transaction was executed. Is used as part of the record metadata.
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
		keyrecord.setSchemaId(handler.getKeySchemaId());
		for (Field f : keyrecord.getSchema().getFields()) {
			keyrecord.put(f.name(), valuerecord.get(f.name()));
		}
		addRow(topic, partition, handler, keyrecord, valuerecord, changetype, sourceRowID, sourceSystemID);
	}


	/**
	 * The actual internal implementation of how to add the record into the queue. 
	 * It is protected as it should never be called directly but via this {@link #addRow(TopicHandler, Integer, SchemaHandler, JexlRecord, JexlRecord, RowType, String, String) addRow} version.
	 * 
	 * @param topic this record should be put into
	 * @param partition optional partition information
	 * @param handler SchemaHandler with the latest schema IDs
	 * @param keyrecord Avro record of the key
	 * @param valuerecord Avro record of the value
	 * @throws IOException in case anything goes wrong
	 */
	protected abstract void addRowImpl(T topic, Integer partition, SchemaHandler handler, JexlRecord keyrecord, JexlRecord valuerecord) throws IOException;

	
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
	 * Called to tell the source was initial loaded
	 * 
	 * @param schemaname of the table initial loaded
	 * @param producerinstance ID of the producer instance
	 * @param rowcount number of rows loaded
	 * @throws IOException in case anything goes wrong
	 */
	public abstract void confirmInitialLoad(String schemaname, int producerinstance, long rowcount) throws IOException;

	/**
	 * Called to tell that the source initial load was started
	 * 
	 * @param schemaname of the table initial loaded
	 * @param producerinstance ID of the producer instance
	 * @throws IOException in case anything goes wrong
	 */
	public abstract void markInitialLoadStart(String schemaname, int producerinstance) throws IOException;

	/**
	 * Update the Delta information log that this source transaction has been fully loaded
	 *  
	 * @param producerinstance
	 * @throws IOException
	 */
	public abstract void confirmDeltaLoad(int producerinstance) throws IOException;

}
