package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import java.util.List;

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

	public void setTopics(List<String> topics) {
		this.topics = topics;
	}

}
