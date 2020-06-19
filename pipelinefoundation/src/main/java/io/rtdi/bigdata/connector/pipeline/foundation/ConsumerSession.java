package io.rtdi.bigdata.connector.pipeline.foundation;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerState;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.OperationState;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;

/**
 * The ConsumerSession is the connection to the server for consuming records from it.
 * Associated with this class are the topics and schemas consumed for metadata reasons (impact lineage) and to cache the definitions.
 *
 * @param <T> TopicHandler
 */
public abstract class ConsumerSession<T extends TopicHandler> implements ISchemaRegistrySource {
	private ConsumerProperties properties;
	private IPipelineBase<?, T> api = null;
	private Map<String, T> topichandlers = new HashMap<>();
	private Cache<Integer, Schema> schemaidcache = Caffeine.newBuilder().expireAfterAccess(Duration.ofMinutes(30)).maximumSize(1000).build();
	private long lastmetadatachange = 0;
	protected Logger logger;
	protected OperationState state;
	protected long lastoffset = -1;
	protected String lasttopic = null;
	private IControllerState controller;
	private String tenantid;

	protected ConsumerSession(ConsumerProperties properties, String tenantid, IPipelineBase<?, T> api) throws PropertiesException {
		super();
		if (properties == null) {
			throw new PropertiesException("The ConsumerSession needs a valid PipelineConsumerProperties object to know the topics to listen on");
		}
		this.properties = properties;
		this.api = api;
		logger = LogManager.getLogger(this.getClass().getName());
		this.tenantid = tenantid;
	}

	
	/**
	 * @param processor for transforming the row structure during a fetch
	 * @return number of rows fetched within that time interval or the row count upper limit got reached
	 * @throws IOException in case of any error
	 */
	public abstract int fetchBatch(IProcessFetchedRow processor) throws IOException;

	/**
	 * @return The consumer properties from the constructor 
	 */
	public ConsumerProperties getProperties() {
		return properties;
	}
	
	@Override
	public Schema getSchema(int schemaid) throws PropertiesException {
		Schema schema = schemaidcache.getIfPresent(schemaid);
		if (schema == null) {
			schema = api.getSchema(schemaid);
			if (schema != null) {
				schemaidcache.put(schemaid, schema);
				return schema;
			} else {
				throw new PipelineRuntimeException("The schemaid \"" + String.valueOf(schemaid) + "\" is not known in the schema registry");
			}
		} else {
			return schema;
		}
	}

	/**
	 * A ConsumerSession can maintain the list of topics it reads from either once, in this method, or dynamically.
	 * This information is used for metadata.
	 *  
	 * Use addTopic(TopicHandler) to actually add the individual topics.
	 * 
	 * @throws PropertiesException in case the topics cannot be set
	 */
	public abstract void setTopics() throws PropertiesException;
	
	/**
	 * Note: This method returns something useful only after the {@link #open()} had been called. For some implementations it might happen earlier 
	 * but most need to establish a connection first.
	 * 
	 * @return The list of all topics it does listen on. Set by {@link #setTopics()}.
	 *
	 */
	public Map<String, T> getTopics() {
		return topichandlers;
	}
		
	/**
	 * This method adds a new TopicHandler of a topic the consumer is listening on. 
	 * It builds a directory of all topics this consumer does create data for and can be used to quickly get a TopicHandler.
	 * 
	 * @param topichandler TopicHandler
	 */
	protected void addTopic(T topichandler) {
		topichandlers.put(topichandler.getTopicName().getName(), topichandler);
		lastmetadatachange = System.currentTimeMillis();
	}

	/**
	 * Inverse operation to {@link #addTopic(TopicHandler)}
	 * 
	 * @param topichandler TopicHandler
	 */
	protected void removeTopic(T topichandler) {
		lastmetadatachange = System.currentTimeMillis();
		topichandlers.remove(topichandler.getTopicName().getName());
	}

	/**
	 * Get the TopicHandler based on the TopicName. Useful when a consumer gets data from different tables, then this
	 * method can be used. Or maybe the {@link #getTopic(String)}?<br>
	 * 
	 * @param topicname Name of the topic
	 * @return TopicHandler
	 */
	public T getTopic(TopicName topicname) {
		return topichandlers.get(topicname.getName());
	}
	
	/**
	 * Get the TopicHandler based on the TopicName.getName() portion. 
	 * Often the TopicName.getName() will be the source database table name, hence when a change row is found for a table,
	 * by using this method the TopicHandler can be found.<br>
	 *  
	 * @param topicname Name of the topic as string
	 * @return TopicHandler
	 */
	public T getTopic(String topicname) {
		return topichandlers.get(topicname);
	}
	
	/**
	 * @throws IOException In case anything went wrong
	 */
	public abstract void open() throws IOException;

	/**
	 * Implement this method to close the topic server connection
	 */
	public abstract void close();

	/**
	 * This method implementation should confirm for the Consumer that the data has been processed successfully up to this point
	 * 
	 * @throws IOException In case anything went wrong
	 */
	public abstract void commit() throws IOException;

	public long getLastMetadataChange() {
		return lastmetadatachange;
	}
	
	@Override
	public String toString() {
		return "ConsumerSession " + properties.getName();
	}

	public OperationState getOperationState() {
		return state;
	}
	
	public long getLastOffset() {
		return lastoffset;
	}
	
	public String getLastTopic() {
		return lasttopic;
	}


	public void setController(IControllerState controller) {
		this.controller = controller;
	}
	
	public ControllerState getControllerState() {
		if (controller != null) {
			return controller.getState();
		} else {
			return ControllerState.STARTED;
		}
	}
	
	public boolean isRunning() {
		if (controller != null) {
			return controller.isRunning();
		} else {
			return true;
		}
	}

	public IPipelineBase<?, T> getPipelineAPI() {
		return api;
	}

	public String getTenantId() {
		return tenantid;
	}

}
