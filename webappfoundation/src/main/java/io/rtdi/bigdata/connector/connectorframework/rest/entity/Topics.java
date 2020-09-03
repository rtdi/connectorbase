package io.rtdi.bigdata.connector.connectorframework.rest.entity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;

import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;

public class Topics {
	private ArrayList<Topic> topics;

	public Topics() {
		super();
	}
		
	public Topics(IPipelineAPI<?, ?, ?, ?> api) throws IOException {
		List<TopicName> entities = api.getTopics();
		topics = new ArrayList<Topic>(entities.size());
		for (TopicName entity : entities) {
			Topic data = new Topic(entity.getName());
			topics.add(data);
		}
	}

	@XmlElement
	public ArrayList<Topic> getTopics() {
		return topics;
	}


	public static class Topic {

		private String topicname;
		
		public Topic() {
		}

		public Topic(String entity) {
			this.topicname = entity;
		}

		@XmlElement
		public String getTopicname() {
			return topicname;
		}

	}
}
