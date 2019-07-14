package io.rtdi.bigdata.connector.connectorframework;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.controller.Controller;
import io.rtdi.bigdata.connector.connectorframework.controller.ProducerController;
import io.rtdi.bigdata.connector.connectorframework.controller.ProducerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.entity.SchemaMappingData;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorRuntimeException;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorTemporaryException;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.ProducerSession;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RowType;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;
import io.rtdi.bigdata.connector.pipeline.foundation.mapping.RecordMapping;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.KeySchema;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;

/**
 * 
 * The Producer class is the main code to implement for producing data.<BR/>
 * The code sequence is
 * <UL><LI>constructor: connect to the source system</LI>
 * <LI>createTopiclist(): Create the initial list of topics and their schemas the producer will use</LI>
 * <LI>startProducerImpl(): Code that has to happen right before the start of the capturing  
 * <LI>getLastSourceTransaction(): check if the producer is new or got restarted</LI>
 * <UL><LI>null?: call initialLoad()</LI>
 * <LI>else?: call restartWith()</LI></UL>
 * <LI>poll()...</LI>
 * <LI>poll(): Call the poll() method in a loop until the connector is signaled to stopped.</LI>
 * <LI>poll()...</LI>
 * <LI>closeImpl(): Disconnect</LI></UL>
 *
 * @param <S> ConnectionProperties
 * @param <P> ProducerProperties
 */
public abstract class Producer<S extends ConnectionProperties, P extends ProducerProperties> implements Closeable {

	private ProducerSession<?> producersession;
	private ProducerInstanceController instance;
	protected final Logger logger;
	protected ArrayBlockingQueue<Data> pollqueue = new ArrayBlockingQueue<>(10000);
	private Controller<?> executor;
	protected Data commit = new Data();
	private Map<String, Object> transactions = new HashMap<>();


	public Producer(ProducerInstanceController instance) throws PropertiesException {
		producersession = instance.getPipelineAPI().createNewProducerSession(instance.getProducerProperties());
		producersession.open();
		this.instance = instance;
		logger = LogManager.getLogger(this.getClass().getName());
	}

	/**
	 * Allows the connector implementation to execute code at producer start, to start logging changes in the database.
	 * @throws IOException
	 */
	public abstract void startProducerChangeLogging() throws IOException;

	/**
	 * From this point on the initial load or error recovery is completed and the normal creation of change data should start
	 * @throws IOException
	 */
	public abstract void startProducerCapture() throws IOException;

	/**
	 * In this code segment the connector should create/read all used topics and their schemas.<BR/>
	 * This can end up as either of two options for topic and schema each:
	 * <OL><LI>Check if the topic/schema exists or create it</LI>
	 * <LI>Check if the topic/schema exists or fail</LI></OL>
	 * 
	 * This information is cached so the Producer has a quick way to access the topic/schema. And it provides 
	 * the information who writes into what topics with which schema.<BR/>
	 * 
	 * This method has to invoke {@link #addTopicSchema(TopicHandler, SchemaHandler)}.
	 * 
	 * @see ProducerInstanceController#addTopic(TopicHandler)
	 * @see #addTopic(TopicHandler)
	 * @see #addTopicSchema(TopicHandler, SchemaHandler)
	 * 
	 * @throws PropertiesException
	 */
	public abstract void createTopiclist() throws IOException;

	/**
	 * @return null or the point as transactionid where the producer should start reading the source system
	 * @throws IOException
	 */
	public abstract String getLastSuccessfulSourceTransaction() throws IOException;

	/**
	 * In case the producer is started for the first time, this method is called and
	 * should send all the source data.
	 * 
	 * @throws PropertiesException
	 */
	public abstract void initialLoad() throws IOException;

	/**
	 * In case the producer is restarted after an error, this method is called so the
	 * producer can send all changes since this transactionid.
	 * 
	 * @param lastsourcetransactionid where to start from
	 * @throws IOException
	 * 
	 * @see #getLastSuccessfulSourceTransaction()
	 */
	public abstract void restartWith(String lastsourcetransactionid) throws IOException;
	
	/**
	 * The poll method is supposed to be a blocking call that exits either when
	 * 10'000 records have been produced or one second passed.<BR/>
	 * The record limit is needed to handle memory consumption, the time limit to update the
	 * variables and to allow a clean exit in case of a shutdown.
	 * 
	 * The default implementation is using a BlockingQueue.<br/>
	 * One thread does produce data constantly and is blocked when adding records to the queue. The poll method reads
	 * the queue for up to one second or when enough records have been received. {@link #startLongRunningProducer(Runnable)}
	 * 
	 * @return Number of records produced in this cycle
	 * @throws IOException 
	 */
	public int poll() throws IOException {
		int rows = 0;
		Data data;
		try {
			while ((data = pollqueue.poll(1, TimeUnit.SECONDS)) != null && rows < 10000) {
				if (executor == null || !executor.isRunning()) {
					throw new ConnectorTemporaryException("Long Running executor terminated, nobody producing rows for the internal queue any longer");
				}
				if (producersession.getTransactionID() == null) {
					logger.info("poll starts transaction");
					producersession.beginTransaction(data.sourcetransactionid);
				}
				if (data == commit) {
					String t = producersession.getTransactionID();
					producersession.commitTransaction();
					logger.info("poll received commit");
					commit(t, transactions.get(t));
					transactions.remove(t);
				} else {
					logger.info("poll received record {}", data.valuerecord.toString());
					
					JexlRecord targetvaluerecord;
					if (data.schemahandler.getMapping() != null) {
						targetvaluerecord = data.schemahandler.getMapping().apply(data.valuerecord);
					} else {
						targetvaluerecord = data.valuerecord;
					}
					JexlRecord keyrecord = new JexlRecord(data.schemahandler.getKeySchema());
					for (Field f : keyrecord.getSchema().getFields()) {
						keyrecord.put(f.name(), targetvaluerecord.get(f.name()));
					}

					producersession.addRow(data.topichandler, data.partition, data.schemahandler,
							keyrecord, targetvaluerecord, data.changetype, data.sourcerowid, data.sourcesystemid);
					rows++;
				}
			}
		} catch (InterruptedException e) {
			logger.info("Polling the source got interrupted");
		}
		return rows;
	}

	public void queueBeginTransaction(String transactionid, Object payload) {
		transactions.put(transactionid, payload);
	}
	
	public void queueCommitRecord() {
		try {
			pollqueue.put(commit);
		} catch (InterruptedException e) {
			logger.info("Adding a record to the queue got interrupted");
		}
	}
	
	public void waitTransactionsCompleted() {
		while (transactions.size() != 0) {
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				return;
			}
		}
	}
	
	public void commit(String sourcetransactionid, Object payload) throws ConnectorRuntimeException {
	}

	public void queueRecord(TopicHandler topichandler, Integer partition, SchemaHandler schemahandler,
			JexlRecord valuerecord, RowType changetype, 
			String sourcerowid, String sourcesystemid, String sourcetransactionid) {
		Data data = new Data(topichandler, partition, schemahandler, valuerecord, 
				changetype, sourcerowid, sourcesystemid, sourcetransactionid);
		try {
			pollqueue.put(data);
		} catch (InterruptedException e) {
			logger.info("Adding a record to the queue got interrupted");
		}
	}
	
	/**
	 * In order to simplify the implementation, producers can use this method to produce data for the poll method.
	 * The idea is that any potentially long running procedure passes a Runnable into this method at the start and the runnable
	 * is in an endless loop reading data by constantly scanning the source. If the producer is shutdown, this thread will get an interrupt signal
	 * and should terminate asap.
	 * 
	 * @param task
	 * @throws IOException 
	 */
	public void startLongRunningProducer(Controller<?> task) throws IOException {
		instance.addChild(task.getName(), task);
		this.executor = task;
		task.startController();
	}

	/**
	 * The {@link #poll()} calls are either blocking, meaning they themselves wait for data or return asap.
	 * The time the process waits between two poll calls is returned by this method here.
	 * 
	 * @return number of ms to wait between polls from the source system
	 */
	public abstract long getPollingInterval();
	
	@Override
	public void close() {
		closeImpl();
		instance.removeChildControllers();
		producersession.close();
	}
	
	/**
	 * Place to execute code during the close operation
	 */
	public abstract void closeImpl();

	/**
	 * @return convenience function to return the instance controller's ConnectionProperties
	 */
	@SuppressWarnings("unchecked")
	public S getConnectionProperties() {
		return (S) instance.getConnectionProperties();
	}

	/**
	 * @return convenience function to return the instance controller's PipelineAPI
	 */
	public IPipelineAPI<?, ?, ?, ?> getPipelineAPI() {
		return instance.getPipelineAPI();
	}

	/**
	 * @return convenience function to return the instance controller's ConnectionProperties
	 */
	public ProducerSession<?> getProducerSession() {
		return producersession;
	}
	
	/**
	 * Convenience function to add a topic to the instance controller
	 * @see ProducerInstanceController
	 */
	public void addTopic(TopicHandler topichandler) throws IOException {
		instance.addTopic(topichandler);
	}
	
	/**
	 * Convenience function to remove a topic from the instance controller
	 * @see ProducerInstanceController
	 */
	public void removeTopic(TopicHandler topic) throws IOException {
		instance.removeTopic(topic);
	}
	
	/**
	 * @return convenience function to ask the instance controller for a specific topic
	 * @see ProducerInstanceController
	 */
	public TopicHandler getTopic(TopicName topicname) {
		return instance.getTopic(topicname);
	}
	
	public RecordMapping getMapping(String sourceschemaname) throws IOException {
		File mappingfile = SchemaMappingData.getLastActiveMapping(getConnectorController(), getConnectionController().getName(), sourceschemaname);
		if (mappingfile != null) {
			String targetschemaname = RecordMapping.getTargetSchemaname(mappingfile);
			SchemaHandler schemahandler = getPipelineAPI().getSchema(targetschemaname);
			if (schemahandler == null) {
				try {
					Schema targetvalueschema = SchemaMappingData.getTargetSchema(mappingfile);
					Schema targetkeyschema = KeySchema.create(targetvalueschema);
					schemahandler = getPipelineAPI().registerSchema(targetschemaname, null, targetkeyschema, targetvalueschema);
				} catch (SchemaException e) {
					throw new ConnectorRuntimeException("Creating the value schema failed", e, null, sourceschemaname);
				}
			}
			return new RecordMapping(mappingfile, schemahandler);
		} else {
			return null;
		}
	}
	
	public SchemaHandler getSchemaHandler(String sourceschemaname) throws IOException {
		RecordMapping mapping = getMapping(sourceschemaname);
		if (mapping != null) {
			return mapping.getOutputSchemaHandler();
		} else {
			// There is no mapping, use the schema directly
			SchemaHandler schemahandler = getPipelineAPI().getSchema(sourceschemaname);
			if (schemahandler == null) {
				Schema valueschema = null;
				try {
					valueschema = createSchema(sourceschemaname);
					Schema keyschema = KeySchema.create(valueschema);
					schemahandler = getPipelineAPI().registerSchema(sourceschemaname, null, keyschema, valueschema);
				} catch (SchemaException e) {
					throw new ConnectorRuntimeException("Creating the value schema failed", e, null, sourceschemaname);
				}
			}
			return schemahandler;
		}
	}

	
	protected abstract Schema createSchema(String sourceschemaname) throws SchemaException, IOException;

	/**
	 * Convenience function to add a topic/schema to the instance controller
	 * @see ProducerInstanceController
	 */
	public void addTopicSchema(TopicHandler topic, SchemaHandler schema) throws PipelineRuntimeException {
		instance.addTopicSchema(topic, schema);
	}
	
	/**
	 * Convenience function remove a schema from the instance controller
	 * @see ProducerInstanceController
	 */
	public void removeTopicSchema(TopicHandler topic, SchemaHandler schema) throws PipelineRuntimeException {
		instance.removeTopicSchema(topic, schema);
	}
	
	/**
	 * @return convenience function to ask the instance controller for a specific schema
	 * @see ProducerInstanceController
	 */
	public SchemaHandler getSchema(String schemaname) throws PipelineRuntimeException {
		return instance.getSchema(schemaname);
	}
	
	/**
	 * @return convenience function to ask the instance controller for a topics and their schemas
	 * 
	 * @see ProducerInstanceController
	 */
	public Map<TopicHandler, Set<SchemaHandler>> getTopics() {
		return instance.getTopics();
	}
	
	/**
	 * @return convenience function to ask the instance controller for a topic
	 * 
	 * @see ProducerInstanceController
	 */
	public TopicHandler getTopic(String topicname) {
		return instance.getTopic(topicname);
	}
	
	@Override
	public String toString() {
		return "Producer " + instance.getName();
	}
	
	@SuppressWarnings("unchecked")
	public P getProducerProperties() {
		return (P) instance.getProducerProperties();
	}
	
	public ProducerInstanceController getProducerInstance() {
		return instance;
	}

	public ConnectionController getConnectionController() {
		return instance.getConnectionController();
	}

	public ProducerController getProducerController() {
		return instance.getProducerController();
	}

	public ConnectorController getConnectorController() {
		return instance.getConnectorController();
	}

	private static class Data {
		private TopicHandler topichandler;
		private Integer partition;
		private SchemaHandler schemahandler;
		private JexlRecord valuerecord;
		private RowType changetype;
		private String sourcerowid;
		private String sourcesystemid;
		private String sourcetransactionid;

		private Data() {	
		}
		
		private Data(TopicHandler topichandler, Integer partition, SchemaHandler schemahandler,
				JexlRecord valuerecord, RowType changetype, 
				String sourcerowid, String sourcesystemid, String sourcetransactionid) {
			this.topichandler = topichandler;
			this.partition = partition;
			this.schemahandler = schemahandler;
			this.valuerecord = valuerecord;
			this.changetype = changetype;
			this.sourcerowid = sourcerowid;
			this.sourcesystemid = sourcesystemid;
			this.sourcetransactionid = sourcetransactionid;
		}

	}
	
}
