package io.rtdi.bigdata.connector.pipeline.foundation;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.metadata.subelements.TopicMetadata;

/**
 * The TopicHandler provides access to a topic in the server.
 *
 */
public class TopicHandler implements Comparable<TopicHandler> {
	
	private TopicName topicname;
	private TopicMetadata metadata = null;
	
	/**
	 * Create a new TopicHandler based on the TenantID and the tenant specific topic name.
	 * 
	 * @param name tenant specific topic name
	 * @param topicmetadata optional topic metadata
	 * @throws PropertiesException in case the name is invalid
	 */
	public TopicHandler(String name, TopicMetadata topicmetadata) throws PropertiesException {
		this(new TopicName(name), topicmetadata); // validates that name is not null
	}

	/**
	 * Create a new TopicHandler object based on the global topic name.
	 * 
	 * @param topicname as TopicName object
	 * @param topicmetadata optional topic metadata
	 * @throws PropertiesException in case the name is invalid
	 */
	public TopicHandler(TopicName topicname, TopicMetadata topicmetadata) throws PropertiesException {
		if (topicname == null) {
			throw new PropertiesException("topicname cannot be null");
		}
		if (topicmetadata == null) {
			throw new PropertiesException("topic metadata cannot be null");
		}
		this.topicname = topicname;
	}

	/**
	 * A secondary constructor for topic servers that do not have metadata.
	 * 
	 * @param topicname as TopicName object
	 * @param partitions of the topic
	 * @param replicationfactor of the topic
	 * @throws PropertiesException if the topicname is null
	 */
	protected TopicHandler(TopicName topicname, int partitions, int replicationfactor) throws PropertiesException {
		if (topicname == null) {
			throw new PropertiesException("topicname cannot be null");
		}
		this.topicname = topicname;
		metadata = new TopicMetadata(partitions, replicationfactor);
	}

	@Override
	public int hashCode() {
		return topicname.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		} else if (obj instanceof TopicHandler) {
			TopicHandler t = (TopicHandler) obj;
			return topicname.equals(t.getTopicName());
		} else {
			return false;
		}
	}

	/**
	 * @return The topic name
	 */
	public TopicName getTopicName() {
		return topicname;
	}
	
	/**
	 * @return topic metadata
	 */
	public TopicMetadata getTopicMetadata() {
		return this.metadata;
	}
	
	@Override
	public String toString() {
		return topicname.toString();
	}

	@Override
	public int compareTo(TopicHandler q) {
		if (q == null) {
			return +1;
		} else {
			return topicname.toString().compareTo(q.topicname.toString());
		}
	}	
	
}
