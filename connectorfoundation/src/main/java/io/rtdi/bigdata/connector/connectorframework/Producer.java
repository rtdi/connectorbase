package io.rtdi.bigdata.connector.connectorframework;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.controller.ProducerController;
import io.rtdi.bigdata.connector.connectorframework.controller.ProducerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.entity.SchemaMappingData;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.ProducerSession;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;
import io.rtdi.bigdata.connector.pipeline.foundation.mapping.RecordMapping;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.KeySchema;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;

/**
 * 
 * The Producer class is the main code to implement for producing data.<br>
 * The code sequence is
 * <ul><li>constructor: connect to the source system</li>
 * <li>createTopiclist(): Create the initial list of topics and their schemas the producer will use</li>
 * <li>startProducerImpl(): Code that has to happen right before the start of the capturing  
 * <li>getLastSourceTransaction(): check if the producer is new or got restarted
 *   <ul><li>null?: call initialLoad()</li>
 *   <li>else?: call restartWith()</li></ul>
 * </li>
 * <li>poll()...</li>
 * <li>poll(): Call the poll() method in a loop until the connector is signaled to stopped.</li>
 * <li>poll()...</li>
 * <li>closeImpl(): Disconnect</li></ul>
 *
 * @param <S> ConnectionProperties
 * @param <P> ProducerProperties
 */
public abstract class Producer<S extends ConnectionProperties, P extends ProducerProperties> implements Closeable {

	protected ProducerSession<?> producersession;
	protected ProducerInstanceController instance;
	protected final Logger logger;


	public Producer(ProducerInstanceController instance) throws PropertiesException {
		producersession = instance.getPipelineAPI().createNewProducerSession(instance.getProducerProperties());
		producersession.open();
		this.instance = instance;
		logger = LogManager.getLogger(this.getClass().getName());
	}

	/**
	 * Allows the connector implementation to execute code at producer start, to start logging changes in the database.
	 * @throws IOException if error
	 */
	public abstract void startProducerChangeLogging() throws IOException;

	/**
	 * From this point on the initial load or error recovery is completed and the normal creation of change data should start
	 * @throws IOException if error
	 */
	public abstract void startProducerCapture() throws IOException;

	/**
	 * In this code segment the connector should create/read all used topics and their schemas.<br>
	 * This can end up as either of two options for topic and schema each:
	 * <ol><li>Check if the topic/schema exists or create it</li>
	 * <li>Check if the topic/schema exists or fail</li></ol>
	 * 
	 * This information is cached so the Producer has a quick way to access the topic/schema. And it provides 
	 * the information who writes into what topics with which schema.<br>
	 * 
	 * This method has to invoke {@link #addTopicSchema(TopicHandler, SchemaHandler)}.
	 * @throws IOException if error
	 * 
	 * @see ProducerInstanceController#addTopic(TopicHandler)
	 * @see #addTopic(TopicHandler)
	 * @see #addTopicSchema(TopicHandler, SchemaHandler)
	 * 
	 * @throws PropertiesException if error
	 */
	public abstract void createTopiclist() throws IOException;

	/**
	 * @return null or the point as transactionid where the producer should start reading the source system
	 * @throws IOException if error
	 */
	public abstract String getLastSuccessfulSourceTransaction() throws IOException;

	/**
	 * In case the producer is started for the first time, this method is called and
	 * should send all the source data.
	 * @throws IOException if error
	 * 
	 * @throws PropertiesException if error
	 */
	public abstract void initialLoad() throws IOException;

	/**
	 * In case the producer is restarted after an error, this method is called so the
	 * producer can send all changes since this transactionid.
	 * 
	 * @param lastsourcetransactionid where to start from
	 * @throws IOException if error
	 * 
	 * @see #getLastSuccessfulSourceTransaction()
	 */
	public abstract void restartWith(String lastsourcetransactionid) throws IOException;
	
	/**
	 * The poll method is supposed to be a blocking call that exits either when
	 * 10'000 records have been produced or one second passed.<br>
	 * The record limit is needed to handle memory consumption, the time limit to update the
	 * variables and to allow a clean exit in case of a shutdown.
	 * 
	 * @param aftersleep indicates if the previous poll call returned zero records and hence the sleep for poll interval did happen 
	 * @return Number of records produced in this cycle
	 * @throws IOException if error
	 */
	public abstract int poll(boolean aftersleep) throws IOException;
	
	public void commit(String sourcetransactionid, Object payload) throws ConnectorRuntimeException {
	}

	/**
	 * The {@link #poll(boolean)} calls are either blocking, meaning they themselves wait for data or return asap.
	 * The time the process waits between two poll calls is returned by this method here.
	 * 
	 * @return number of ms to wait between polls from the source system
	 */
	public abstract long getPollingInterval();
	
	@Override
	public void close() {
		closeImpl();
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
	 * 
	 * @param topichandler TopicHandler to add
	 * @throws IOException if error
	 * @see ProducerInstanceController
	 */
	public void addTopic(TopicHandler topichandler) throws IOException {
		instance.addTopic(topichandler);
	}
	
	/**
	 * Convenience function to remove a topic from the instance controller
	 * 
	 * @param topic TopicHandler to remove
	 * @throws IOException if error
	 * @see ProducerInstanceController
	 */
	public void removeTopic(TopicHandler topic) throws IOException {
		instance.removeTopic(topic);
	}
	
	/**
	 * @param topicname TopicName
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
			SchemaHandler schemahandler = null; // getPipelineAPI().getSchema(sourceschemaname);
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

	
	/**
	 * If a schema does not exist by name yet, here is the option to automatically create it.
	 * 
	 * @param sourceschemaname Name of the schema to create
	 * @return Schema of the value 
	 * @throws SchemaException in case the schema has a logical error
	 * @throws IOException any other type of error
	 */
	protected abstract Schema createSchema(String sourceschemaname) throws SchemaException, IOException;

	/**
	 * Convenience function to add a topic/schema to the instance controller
	 * 
	 * @param topic TopicHandler
	 * @param schema Schema
	 * @throws PipelineRuntimeException if error
	 * @see ProducerInstanceController
	 */
	public void addTopicSchema(TopicHandler topic, SchemaHandler schema) throws PipelineRuntimeException {
		instance.addTopicSchema(topic, schema);
	}
	
	/**
	 * Convenience function remove a schema from the instance controller
	 * 
	 * @param topic TopicHandler
	 * @param schema Schema
	 * @throws PipelineRuntimeException if error
	 * @see ProducerInstanceController
	 */
	public void removeTopicSchema(TopicHandler topic, SchemaHandler schema) throws PipelineRuntimeException {
		instance.removeTopicSchema(topic, schema);
	}
	
	/**
	 * @param schemaname tenant specific schema name
	 * @return convenience function to ask the instance controller for a specific schema
	 * @throws PipelineRuntimeException if error
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
	 * Convenience function to ask the instance controller for a topic.
	 * 
	 * @param topicname Tenant specific topic name
	 * @return TopicHandler
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
	
}
