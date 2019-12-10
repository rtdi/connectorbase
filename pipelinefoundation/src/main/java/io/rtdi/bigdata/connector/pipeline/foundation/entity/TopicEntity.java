package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;

public class TopicEntity {
	private List<String> schemalist;
	private String topicname;

	public TopicEntity() {
	}
	
	public TopicEntity(String name, Set<SchemaHandler> schemas) {
		this.topicname = name;
		if (schemas != null) {
			schemalist = new ArrayList<>();
			for (SchemaHandler schema : schemas) {
				schemalist.add(schema.getSchemaName().getName());
			}
		}
	}

	public TopicEntity(String name, Set<String> schemas, boolean x) {
		this.topicname = name;
		if (schemas != null) {
			schemalist = new ArrayList<>();
			schemalist.addAll(schemas);
		}
	}

	public List<String> getSchemaList() {
		return schemalist;
	}

	public void setSchemaList(List<String> schemalist) {
		this.schemalist = schemalist;
	}

	public String getTopicName() {
		return topicname;
	}

	public void setTopicName(String topicname) {
		this.topicname = topicname;
	}
}