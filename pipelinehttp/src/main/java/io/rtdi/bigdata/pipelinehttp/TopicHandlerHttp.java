package io.rtdi.bigdata.pipelinehttp;

import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.metadata.subelements.TopicMetadata;

public class TopicHandlerHttp extends TopicHandler {

	public TopicHandlerHttp(String tenant, String name, TopicMetadata topicmetadata) throws PropertiesException {
		super(tenant, name, topicmetadata);
	}

	public TopicHandlerHttp(TopicName topicname, TopicMetadata topicmetadata) throws PropertiesException {
		super(topicname, topicmetadata);
	}

	public TopicHandlerHttp(TopicName topicname, int partitions, int replicationfactor) throws PropertiesException {
		super(topicname, partitions, replicationfactor);
	}

}
