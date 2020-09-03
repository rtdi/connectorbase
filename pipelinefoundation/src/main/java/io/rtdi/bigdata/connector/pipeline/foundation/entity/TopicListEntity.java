package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import java.util.ArrayList;
import java.util.List;

import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;

public class TopicListEntity {

	private List<String> topics;
	
	public TopicListEntity() {
	}

	public TopicListEntity(List<String> topiclist) {
		this();
		this.topics = topiclist;
	}

	public List<String> getTopics() {
		return topics;
	}

	public List<TopicName> topicsAsTopicName() {
		List<TopicName> r = new ArrayList<>();
		if (topics != null) {
			for (String topic : topics) {
				r.add(TopicName.createViaEncoded(topic));
			}
		}
		return r;
	}

	public void setTopics(List<String> topics) {
		this.topics = topics;
	}

}
