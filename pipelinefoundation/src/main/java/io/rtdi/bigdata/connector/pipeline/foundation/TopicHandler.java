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
	 * @param tenant
	 * @param topic tenant specific topic name
	 * @param topicmetadata
	 * @throws PipelinePropertiesException 
	 */
	public TopicHandler(String tenant, String name, TopicMetadata topicmetadata) throws PropertiesException {
		this(new TopicName(tenant, name), topicmetadata); // validates that name is not null
	}

	/**
	 * Create a new TopicHandler object based on the global topic name.
	 * 
	 * @param topicname
	 * @param topicmetadata
	 * @throws PipelinePropertiesException
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
	 * @param topicname
	 * @param partitions
	 * @param replicationfactor
	 * @throws PipelinePropertiesException
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
	 * @return The actual Kafka topic metadata
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
