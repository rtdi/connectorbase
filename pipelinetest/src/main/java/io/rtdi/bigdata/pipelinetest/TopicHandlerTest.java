package io.rtdi.bigdata.pipelinetest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.avro.generic.GenericRecord;

import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicPayload;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.metadata.subelements.TopicMetadata;

public class TopicHandlerTest extends TopicHandler {
	private ConcurrentLinkedQueue<TopicPayload> data = new ConcurrentLinkedQueue<>();
	long offest = 0;

	public TopicHandlerTest(String tenant, String name, TopicMetadata topicmetadata) throws PropertiesException {
		super(tenant, name, topicmetadata);
	}

	public TopicHandlerTest(TopicName topicname, TopicMetadata topicmetadata) throws PropertiesException {
		super(topicname, topicmetadata);
	}

	public TopicHandlerTest(TopicName topicname, int partitions, int replicationfactor) throws PropertiesException {
		super(topicname, partitions, replicationfactor);
	}

	public void addData(GenericRecord keyrecord, GenericRecord valuerecord, int keyschemaid, int valueschemaid) {
		TopicPayload p = new TopicPayload(super.getTopicName(), offest++, 0, System.currentTimeMillis(), keyrecord, valuerecord, keyschemaid, valueschemaid);
		data.add(p);
		while (data.size() > 1000) {
			data.remove();
		}
	}
	
	public List<TopicPayload> getData() {
		ArrayList<TopicPayload> ret = new ArrayList<TopicPayload>();
		ret.addAll(data);
		return ret;
	}
}
