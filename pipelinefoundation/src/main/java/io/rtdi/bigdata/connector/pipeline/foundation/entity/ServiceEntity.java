package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;

public class ServiceEntity {
	private List<String> consuming;
	private List<TopicEntity> producing;
	private String servicename;
	private String hostname;
	private String apiconnection;

	public ServiceEntity() {
	}
	
	public ServiceEntity(String name, IPipelineAPI<?,?,?,?> api, Map<String, ? extends TopicHandler> consumertopics, Map<TopicHandler, Set<SchemaHandler>> producertopics) {
		this.servicename = name;
		this.hostname = api.getHostName();
		this.apiconnection = api.getConnectionLabel();
		if (consumertopics != null) {
			consuming = new ArrayList<>();
			for (String topicname : consumertopics.keySet()) {
				consuming.add(topicname);
			}
		}
		if (producertopics != null) {
			producing = new ArrayList<>();
			for (TopicHandler topicname : producertopics.keySet()) {
				Set<SchemaHandler> m = producertopics.get(topicname);
				producing.add(new TopicEntity(topicname.getTopicName().getName(), m));
			}
		}
	}
	
	public ServiceEntity(String name, String hostname, String apilabel, List<String> consumertopics, Map<String, Set<String>> producertopics) {
		this.servicename = name;
		this.hostname = hostname;
		this.apiconnection = apilabel;
		consuming = consumertopics;
		if (producertopics != null) {
			producing = new ArrayList<>();
			for (String topicname : producertopics.keySet()) {
				Set<String> m = producertopics.get(topicname);
				producing.add(new TopicEntity(topicname, m, true));
			}
		}
	}

	public List<TopicEntity> getProducedTopicList() {
		return producing;
	}

	public void setProducedTopicList(List<TopicEntity> topiclist) {
		this.producing = topiclist;
	}

	public List<String> getConsumedTopicList() {
		return consuming;
	}

	public void setConsumedTopicList(List<String> consuming) {
		this.consuming = consuming;
	}

	public String getServiceName() {
		return servicename;
	}

	public void setServiceName(String servicename) {
		this.servicename = servicename;
	}

	public String getApiconnection() {
		return apiconnection;
	}

	public void setApiconnection(String apiconnection) {
		this.apiconnection = apiconnection;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

}