package io.rtdi.bigdata.connector.connectorframework.controller;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.rtdi.bigdata.connector.connectorframework.IConnectorFactoryProducer;
import io.rtdi.bigdata.connector.connectorframework.Producer;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ShutdownException;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.PipelineAbstract;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.LoadInfo;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ServiceEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerExitType;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.OperationState;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;

public class ProducerInstanceController extends ThreadBasedController<Controller<?>> {

	private ProducerController producercontroller;
	private String lastsourcetransactionid;
	private OperationState operationstate;
	private int pollcalls = 0;
	
	/**
	 * A nested structure containing the impact/lineage information of topics and their schemas being created by this ProducerSession.
	 */
	private Map<TopicHandler, Set<SchemaHandler>> usedtopics = new HashMap<>();
	/**
	 * A cache with all topichandlers based on their TopicName.getName().
	 */
	private Map<String, TopicHandler> topichandlers = new HashMap<>();
	/**
	 * A cache with all schemahandlers based on their SchemaName.getName().
	 */
	private Map<String, SchemaHandler> schemahandlers = new HashMap<>();
	
	private boolean ismetadatachanged = false;
	private int instancenumber;
	private boolean updateschemacaches;
	private long rowsprocessed;
	private Long lastdatatimestamp;

	public ProducerInstanceController(String name, ProducerController producercontroller, int instancenumber) {
		super(name);
		this.producercontroller = producercontroller;
		this.instancenumber = instancenumber;
	}

	public String getLastTransactionId() {
		return lastsourcetransactionid;
	}

	@Override
	protected void startThreadControllerImpl() throws PipelineRuntimeException {
	}

	@Override
	protected void stopThreadControllerImpl(ControllerExitType exittype) {
		this.stopChildControllers(ControllerExitType.ABORT);
	}
	
	public OperationState getOperation() {
		return operationstate;
	}
	
	public int getInstanceNumber() {
		return instancenumber;
	}

	@Override
	public void runUntilError() throws IOException {
		try (Producer<?,?> rowproducer = getConnectorFactory().createProducer(this)) {
			try {
				// Allow the producer to create all schemas, topics and their relationships
				rowproducer.createTopiclist();
				// Start capturing changes on all requested source objects 
				rowproducer.startProducerChangeLogging();
				
				/*
				 *  Read the LoadInfo from the transaction topic. This tells what table have been initial 
				 *  loaded already and the last processed delta transaction.
				 */
				Map<String, LoadInfo> loadinfo = rowproducer.getLoadInfo();
				/*
				 * The initial load info is per table, the delta is global.
				 * Imagine situations where the initial load started but only half the tables were loaded. In that case
				 * the initial load should skip the already loaded objects.
				 * The normal case will be that all tables have been initial loaded and the last successful transaction
				 * was 1234, hence all tables are read from this transaction onwards.
				 * Another case would be that all was fine but now a new table got added.
				 */
				LoadInfo delta = loadinfo.get(PipelineAbstract.ALL_SCHEMAS);
				lastsourcetransactionid = null;
				String transactionid_at_start = rowproducer.getCurrentTransactionId();
				logger.debug("TransactionID at start is \"{}\"", transactionid_at_start);
				
				if (delta != null) {
					// The producer had processed delta before and should continue where it left.
					lastsourcetransactionid = delta.getTransactionid();
					logger.debug("TransactionID of the last delta is \"{}\"", lastsourcetransactionid);
				} else {
					logger.debug("No delta ever executed, marking the current transaction id \"{}\" as the start point", transactionid_at_start);
					rowproducer.beginDeltaTransaction(transactionid_at_start, instancenumber);
					rowproducer.commitDeltaTransaction();
				}
				
				/*
				 * Check if all current tables had been initial loaded already, maybe the user added a new one?
				 */
				List<String> schemanames = rowproducer.getAllSchemas();
				if (schemanames != null) {
					for (String name : schemanames) {
						if (!loadinfo.containsKey(name)) {
							// Initial load all tables the initial load was not yet complete.
							logger.debug("Initial load for table \"{}\" with transaction id \"{}\" is started", name, transactionid_at_start);
							rowproducer.executeInitialLoad(name, transactionid_at_start);
							logger.debug("Initial load for table \"{}\" with transaction id \"{}\" is completed", name, transactionid_at_start);
						}
					}
				}

				if (lastsourcetransactionid != null && lastsourcetransactionid.length() != 0) {
					// Allow the producer to execute some recovery logic
					logger.debug("Execute restart logic with transaction id \"{}\" is started", lastsourcetransactionid);
					rowproducer.restartWith(lastsourcetransactionid);
				} else {
					lastsourcetransactionid = transactionid_at_start;
				}
	
				// A hook for the producer to execute code after initial load/recovery and prior to reading, if needed
				operationstate = OperationState.REQUESTDATA;
				rowproducer.startProducerCapture();
				
				while (isRunning()) {
					operationstate = OperationState.REQUESTDATA;
					// Poll for change data
					logger.debug("Polling all changes starting with transaction id \"{}\"", lastsourcetransactionid);
					lastsourcetransactionid = rowproducer.poll(lastsourcetransactionid);
					logger.debug("Polling all changes completed, all data up to transaction id \"{}\" was sent", lastsourcetransactionid);
					pollcalls++;
					if (updateschemacaches) {
						/*
						 *  Every 20 minutes execute some code. That is to fetch the newest producer schemas in case
						 *  they have changed. Otherwise we would continue producing data with the schema id of and old
						 *  schema. No harm in that, but better to use new schemas if they are present.
						 *  Also allow the producer to execute some housekeeping tasks, e.g. empty log tables.
						 */
						updateSchemaWithLatest();
						rowproducer.executePeriodicTask();
					}
					if (ismetadatachanged) {
						updateLandscape();
					}
					operationstate = OperationState.DONEREQUESTDATA;
					sleep(rowproducer.getPollingInterval()*1000L);
				}
			} catch (ShutdownException e) { // A shutdown signal should silently terminate
				rowproducer.abortTransaction();
			} finally {
				/*
				 * Clear the interrupt flag to ensure the close() of the try operation can close all resources gracefully
				 */
				Thread.interrupted();
			}
		}
	}
	
	/**
	 * Periodically sweep through all schemas and get get the latest versions.
	 * @throws PropertiesException if the schema is invalid
	 */
	public void updateSchemaWithLatest() throws PropertiesException {
		for (Set<SchemaHandler> schemas : usedtopics.values()) {
			Set<SchemaHandler> newschemas = new HashSet<>(); // collect the new schemahandlers here
			for (SchemaHandler schema : schemas) {
				newschemas.add(getPipelineAPI().getSchema(schema.getSchemaName()));
			}
			schemas.addAll(newschemas); // overwrite the current schemahandlers with the new ones
		}
		updateschemacaches = false;
	}

	/**
	 * This producer is generating data for the topic. 
	 * 
	 * @param topichandler TopicHandler to add
	 */
	public void addTopic(TopicHandler topichandler) {
		topichandlers.put(topichandler.getTopicName().getName(), topichandler);
		Set<SchemaHandler> topicschemas = usedtopics.get(topichandler);
		if (topicschemas == null) {
			topicschemas = new HashSet<SchemaHandler>();
			usedtopics.put(topichandler, topicschemas);
		}
		ismetadatachanged = true;
	}

	/**
	 * Inverse operation to {@link #addTopic(TopicHandler)}
	 * 
	 * @param topic TopicHandler to remove
	 * @throws PipelineRuntimeException if error 
	 */
	public void removeTopic(TopicHandler topic) throws PipelineRuntimeException {
		topichandlers.remove(topic.getTopicName().getName());
		usedtopics.remove(topic);
		ismetadatachanged = true;
	}

	/**
	 * Get the TopicHandler based on the TopicName. Useful when a producer gets data from different tables, then this
	 * method can be used. Or maybe the {@link #getTopic(String)}?
	 * 
	 * @param topicname TopicName
	 * @return TopicHandler
	 */
	public TopicHandler getTopic(TopicName topicname) {
		return topichandlers.get(topicname.getName());
	}
	
	/**
	 * Add a schema to an existing topic or add topic and schema at once.
	 * 
	 * @param topic TopicHandler
	 * @param schema SchemaHandler to add
	 */
	public void addTopicSchema(TopicHandler topic, SchemaHandler schema) {
		Set<SchemaHandler> topicschemas = usedtopics.get(topic);
		if (topicschemas == null) {
			addTopic(topic);
			topicschemas = usedtopics.get(topic);
		}
		topicschemas.add(schema);
		schemahandlers.put(schema.getSchemaName().getName(), schema);
		ismetadatachanged = true;
	}
	
	/**
	 * Remove a schema from the provided topic. If the topic does not exist, this operation does nothing.
	 * 
	 * @param topic TopicHandler
	 * @param schema SchemaHandler
	 */
	public void removeTopicSchema(TopicHandler topic, SchemaHandler schema) {
		Set<SchemaHandler> topicschemas = usedtopics.get(topic);
		if (topicschemas != null) {
			topicschemas.remove(schema);
		}
		schemahandlers.remove(schema.getSchemaName().getName());
	}

	/**
	 * Get the SchemaHandler for a schemaname, e.g. a table name.
	 * 
	 * @param schemaname The schema name within the tenant
	 * @return SchemaHandler for the named schema
	 */
	public SchemaHandler getSchema(String schemaname) {
		return schemahandlers.get(schemaname);
	}
	
	/**
	 * @return List of all used topics and their schemas
	 */
	public Map<TopicHandler, Set<SchemaHandler>> getTopics() {
		return usedtopics;
	}

	/**
	 * Get the TopicHandler based on the TopicName.getName() portion. 
	 *  
	 * @param topicname tenant specifc topic name
	 * @return TopicHandler
	 */
	public TopicHandler getTopic(String topicname) {
		return topichandlers.get(topicname);
	}



	@Override
	protected String getControllerType() {
		return "ProducerInstanceController";
	}

	public ProducerProperties getProducerProperties() {
		return producercontroller.getProducerProperties();
	}

	public ConnectionProperties getConnectionProperties() {
		return producercontroller.getConnectionProperties();
	}

	public IPipelineAPI<?, ?, ?, ?> getPipelineAPI() {
		return producercontroller.getPipelineAPI();
	}

	public IConnectorFactoryProducer<?, ?> getConnectorFactory() {
		return (IConnectorFactoryProducer<?, ?>) producercontroller.getConnectorFactory();
	}

	public long getRowsProduced() {
		return rowsprocessed;
	}
	
	public synchronized void incrementRowsProducedBy(long increment) {
		rowsprocessed += increment;
		lastdatatimestamp = System.currentTimeMillis();
	}
	
	public int getPollCalls() {
		return pollcalls;
	}
	
	public ConnectionController getConnectionController() {
		return producercontroller.getConnectionController();
	}
	
	public ProducerController getProducerController() {
		return producercontroller;
	}

	public ConnectorController getConnectorController() {
		return producercontroller.getConnectorController();
	}

	public void removeChildControllers() {
		if (childcontrollers != null) {
			joinChildControllers(ControllerExitType.ABORT); // wait for the children to terminate properly
			// Failsafe in case the children have not been terminated upfront
			stopChildControllers(ControllerExitType.ABORT);
			joinChildControllers(ControllerExitType.ABORT);
			childcontrollers.clear();
		}
	}

	public Long getLastProcessed() {
		return lastdatatimestamp;
	}

	@Override
	protected void updateLandscape() {
		try {
			logger.info("updating producer metadata");
			getPipelineAPI().addProducerMetadata(
					new ProducerEntity(
							producercontroller.getName(),
							producercontroller.getConnectionProperties().getName(),
							getPipelineAPI(),
							usedtopics));
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
			ismetadatachanged = false;
		} catch (IOException e) {
			logger.error("updating producer metadata failed", e);
		}
	}

	@Override
	protected void updateSchemaCache() {
		/*
		 * This just triggers the main loop to execute this action in order to avoid multi-threading side effects
		 */
		this.updateschemacaches = true;
	}

}
