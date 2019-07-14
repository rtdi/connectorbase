package io.rtdi.bigdata.connector.properties;

import java.io.File;
import java.util.List;

import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesRuntimeException;
import io.rtdi.bigdata.connector.properties.atomic.IProperty;
import io.rtdi.bigdata.connector.properties.atomic.PropertyRoot;

/**
 * The TopicListenerProperties is the base class containing the minimal list of properties a Kafka Topic Consumer requires.
 * All specific topic listeners extend this class and add more properties.
 *
 */
public class ConsumerProperties {
	public static final String TOPICLISTENER_TOPICLIST = "topiclistener.topiclist";
	public static final String TOPICLISTENER_INSTANCES = "topiclistener.instances";
	public static final String TOPICLISTENER_FLUSHMS = "topiclistener.flush.max.ms";
	public static final String TOPICLISTENER_MAX_RECORDS = "topiclistener.flush.max.records";

	protected PropertyRoot properties;

	/**
	 * Creates an new TopicListenerProperties with the minimum list of actual properties, some of which have  default value.
	 * 
	 * @param name The name of the TopicListener
	 * @param connectionprops The parent Connection
	 * @throws PropertiesRuntimeException
	 */
	public ConsumerProperties(String name) throws PropertiesException {
		super();
		/* if (name == null || name.length() == 0) {
			throw new PropertiesException("TopicListener has no name set");
		} */
		properties = new PropertyRoot(name);
		properties.addStringProperty(TOPICLISTENER_TOPICLIST, "Topicnames to read", "A regexp matching all topics to read", null, ".*", false);
		properties.addIntegerProperty(TOPICLISTENER_INSTANCES, TOPICLISTENER_INSTANCES, null, null, 1, false);
		properties.addLongProperty(TOPICLISTENER_FLUSHMS, TOPICLISTENER_FLUSHMS, null, null, 60000L, false);
		properties.addIntegerProperty(TOPICLISTENER_MAX_RECORDS, TOPICLISTENER_MAX_RECORDS, null, null, 1000, false);
	}
	
	public ConsumerProperties(String name, TopicName topic) throws PropertiesException {
		this(name);
		this.setTopicPattern(topic.getName());
	}

	public ConsumerProperties(String name, String pattern) throws PropertiesException {
		this(name);
		this.setTopicPattern(pattern);
	}

	/**
	 * @return The name of the TopicListener
	 */
	public String getName() {
		return properties.getName();
	}
	
	/**
	 * @return The regexp of all topics this Listener consumes data from
	 */
	public String getTopicPattern() {
		return properties.getStringPropertyValue(TOPICLISTENER_TOPICLIST);
	}
	
	public int getInstances() {
		return properties.getIntPropertyValue(TOPICLISTENER_INSTANCES);
	}
	
	/**
	 * @return The list of all properties
	 * 
	 * @see PropertyRoot#getValues()
	 */
	public List<IProperty> getValue() {
		return properties.getValues();
	}
		
	/**
	 * Copies the provided values of the ProperyGroup into this object
	 * 
	 * @param pg
	 * @throws PropertiesRuntimeException
	 * 
	 * @see PropertyRoot#parseValue(PropertyRoot)
	 */
	public void setValue(PropertyRoot pg) throws PropertiesException {
		properties.parseValue(pg);
	}

	/**
	 * @return The backing PropertyGroup containing all the values
	 */
	public PropertyRoot getPropertyGroup() {
		return properties;
	}

	/**
	 * Set the InstanceCount property value.
	 * 
	 * @param instances
	 * @throws PipelinePropertiesException 
	 */
	public void setInstanceCount(int instances) throws PropertiesException {
		properties.setProperty(TOPICLISTENER_INSTANCES, instances);
	}
	
	/**
	 * @return The InstanceCount property value
	 */
	public Integer getInstanceCount() {
		return properties.getIntPropertyValue(TOPICLISTENER_INSTANCES);
	}
	
	/**
	 * Set the FlushTime property value.
	 * 
	 * @param flushtime
	 * @throws PipelinePropertiesException 
	 */
	public void setFlushMaxTime(long flushtime) throws PropertiesException {
		properties.setProperty(TOPICLISTENER_FLUSHMS, flushtime);
	}
	
	/**
	 * @return Maximum time a fetch() is supposed to take [ms]
	 */
	public long getFlushMaxTime() {
		return properties.getLongPropertyValue(TOPICLISTENER_FLUSHMS);
	}

	/**
	 * set the list of topics property value
	 * 
	 * @param pattern regexp matching all topics this listener should consume
	 * @throws PipelinePropertiesException 
	 */
	public void setTopicPattern(String pattern) throws PropertiesException {
		properties.setProperty(TOPICLISTENER_TOPICLIST, pattern);
	}

	/**
	 * @return number of records at which a fetch should complete latest
	 */
	public int getFlushMaxRecords() {
		return properties.getIntPropertyValue(TOPICLISTENER_MAX_RECORDS);
	}
	
	/**
	 * Set the FlushTime property value.
	 * 
	 * @param flushtime
	 * @throws PipelinePropertiesException 
	 */
	public void setFlushMaxRecords(int recordcount) throws PropertiesException {
		properties.setProperty(TOPICLISTENER_MAX_RECORDS, recordcount);
	}

	/**
	 * Read the individual connection properties from a directory. The file name is derived from the {@link #getName()}.
	 *  
	 * @param connectionpropertiesfile
	 * @throws PropertiesRuntimeException
	 */
	public void read(File directory) throws PropertiesException {
		properties.read(directory);
	}
	
	/**
	 * Write the current connection properties into a directory. The file name is derived from the {@link #getName()}.
	 *  
	 * @param directory
	 * @throws PropertiesRuntimeException
	 */
	public void write(File directory) throws PropertiesException {
		properties.write(directory);
	}
	
	@Override
	public String toString() {
		if (properties != null) {
			return properties.toString();
		} else {
			return "NULL";
		}
	}

}
