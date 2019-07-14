package io.rtdi.bigdata.connector.connectorframework.controller;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.rtdi.bigdata.connector.connectorframework.IConnectorFactory;
import io.rtdi.bigdata.connector.connectorframework.Producer;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorTemporaryException;
import io.rtdi.bigdata.connector.pipeline.foundation.IOUtils;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerExitType;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerState;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.OperationState;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;

public class ProducerInstanceController extends ThreadBasedController<Controller<?>> {

	private ProducerController producercontroller;
	private String lastsourcetransactionid;
	private OperationState operationstate;
	private long rowsprocessed = 0;
	private int pollcalls = 0;
	private Long lastdatatimestamp = null;
	private long nextmetadatachangecheck = 0;
	private int metadatacheckcount = 0;
	
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

	public ProducerInstanceController(String name, ProducerController producercontroller, int instancenumber) {
		super(name);
		this.producercontroller = producercontroller;
		this.instancenumber = instancenumber;
	}

	@Override
	protected void startThreadControllerImpl() throws PipelineRuntimeException {
	}

	@Override
	protected void stopThreadControllerImpl(ControllerExitType exittype) {
	}
	
	public OperationState getOperation() {
		return operationstate;
	}
	
	public int getInstanceNumber() {
		return instancenumber;
	}

	@Override
	public void runUntilError() throws IOException {
		do {
			try (Producer<?,?> rowproducer = getConnectorFactory().createProducer(this)) {
				rowproducer.createTopiclist();
				rowproducer.startProducerChangeLogging();
				
				lastsourcetransactionid = rowproducer.getLastSuccessfulSourceTransaction();

				if (lastsourcetransactionid == null || lastsourcetransactionid.length() == 0) {
					rowproducer.initialLoad();
				} else {
					rowproducer.restartWith(lastsourcetransactionid);
				}

				operationstate = OperationState.REQUESTDATA;
				rowproducer.startProducerCapture();
				
				while (isRunning() && checkChildren()) {
					operationstate = OperationState.REQUESTDATA;
					int rows = rowproducer.poll();
					rowsprocessed += rows;
					if (rows != 0) {
						lastdatatimestamp = System.currentTimeMillis();
					}
					pollcalls++;
					polliteration();
					operationstate = OperationState.DONEREQUESTDATA;
					// Sleep when no rows have been found, else continue reading the next batch
					if (rows == 0 && (state == ControllerState.STARTED  || state == ControllerState.STARTING)) {
						try {
							Thread.sleep(rowproducer.getPollingInterval());
						} catch (InterruptedException e) {
						}
					}
				}
			} catch (ConnectorTemporaryException e) { 
				errors.addError(e, "Producer got error, retrying in 60s", null);
				logger.error("Producer got error, retrying in 60s", e);
			}
			
			// Only reached in case there is an error - then we retry after a minute - or the thread is to be stopped
			if (state == ControllerState.STARTED) {
				operationstate = OperationState.ERRORWAITFORRETRY;
				try {
					Thread.sleep(60000);
				} catch (InterruptedException e) {
				}
			}
		} while (isRunning());
	}
	
	/**
	 * Periodically sweep through all schemas and get get the latest versions.
	 * @throws PropertiesException 
	 */
	public void updateSchemaWithLatest() throws PropertiesException {
		for (Set<SchemaHandler> schemas : usedtopics.values()) {
			Set<SchemaHandler> newschemas = new HashSet<>(); // collect the new schemahandlers here
			for (SchemaHandler schema : schemas) {
				newschemas.add(getPipelineAPI().getSchema(schema.getSchemaName()));
			}
			schemas.addAll(newschemas); // overwrite the current schemahandlers with the new ones
		}
	}

	/**
	 * This method is called periodically and updates metadata changes.<BR/>
	 * It updates the metadata information in the server whenever there was a change or at least every 12 hours.<BR/>
	 * The latter is needed as outdated metadata will be removed.<BR/>
	 * @throws IOException 
	 */
	public void polliteration() throws IOException {
		if (nextmetadatachangecheck < System.currentTimeMillis()) {
			metadatacheckcount ++;
			if (ismetadatachanged || metadatacheckcount % 60*12 == 0) {
				logger.info("updating metadata");
				getPipelineAPI().addProducerMetadata(
						new ProducerEntity(
								producercontroller.getName(),
								producercontroller.getConnectionProperties().getName(),
								getPipelineAPI(),
								usedtopics));
				ismetadatachanged = false;
			}
			if (metadatacheckcount % 60*48 == 0) {
				updateSchemaWithLatest();
			}
			nextmetadatachangecheck = System.currentTimeMillis() + IOUtils.METADATAREFERSH_CHANGE_CHECK_FREQUENCY;
		}
	}
	
	/**
	 * This producer is generating data for the topic. 
	 * 
	 * @param topichandler
	 * @throws PipelineRuntimeException 
	 */
	public void addTopic(TopicHandler topichandler) throws PipelineRuntimeException {
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
	 * @param topic
	 * @throws IOException 
	 * @throws PipelineRuntimeException 
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
	 * @param topicname
	 * @return TopicHandler
	 */
	public TopicHandler getTopic(TopicName topicname) {
		return topichandlers.get(topicname.getName());
	}
	
	/**
	 * Add a schema to an existing topic or add topic and schema at once.
	 * 
	 * @param topic
	 * @param schema
	 * @throws PipelineRuntimeException
	 */
	public void addTopicSchema(TopicHandler topic, SchemaHandler schema) throws PipelineRuntimeException {
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
	 * @param topic
	 * @param schema
	 * @throws PipelineRuntimeException
	 */
	public void removeTopicSchema(TopicHandler topic, SchemaHandler schema) throws PipelineRuntimeException {
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
	 * @return Schemahandler for the named schema
	 * @throws PipelineRuntimeException
	 */
	public SchemaHandler getSchema(String schemaname) throws PipelineRuntimeException {
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
	 * @param topicname
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

	public IConnectorFactory<?, ?, ?> getConnectorFactory() {
		return producercontroller.getConnectorFactory();
	}

	public long getRowsProduced() {
		return rowsprocessed;
	}
	
	public Long getLastDataTimestamp() {
		return lastdatatimestamp;
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
			joinChildControllers(ControllerExitType.ENDROW); // wait for the children to terminate properly
			// Failsafe in case the children have not been terminated upfront
			stopChildControllers(ControllerExitType.ABORT);
			joinChildControllers(ControllerExitType.ABORT);
			childcontrollers.clear();
		}
	}
}
