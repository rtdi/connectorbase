package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;

public class ConsumerEntity {
	private List<String> topiclist;
	private String consumername;
	private String hostname;
	private String apiconnection;
	private String remoteconnection;

	public ConsumerEntity() {
	}
	
	public ConsumerEntity(String name, String remoteconnection, IPipelineAPI<?,?,?,?> api, Map<String, ? extends TopicHandler> topics) {
		this.consumername = name;
		this.hostname = api.getHostName();
		this.apiconnection = api.getConnectionLabel();
		this.remoteconnection = remoteconnection;
		if (topics != null) {
			topiclist = new ArrayList<>();
			for (String topicname : topics.keySet()) {
				topiclist.add(topicname);
			}
		}
	}

	public ConsumerEntity(String name, String remoteconnection, String hostname, String apilabel, List<String> topics) {
		this.consumername = name;
		this.hostname = hostname;
		this.apiconnection = apilabel;
		this.remoteconnection = remoteconnection;
		topiclist = topics;
	}
	
	public List<String> getTopicList() {
		return topiclist;
	}

	public void setTopicList(List<String> topiclist) {
		this.topiclist = topiclist;
	}

	public String getConsumerName() {
		return consumername;
	}

	public void setConsumerName(String conumername) {
		this.consumername = conumername;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public String getApiconnection() {
		return apiconnection;
	}

	public void setApiconnection(String apiconnection) {
		this.apiconnection = apiconnection;
	}

	public String getRemoteconnection() {
		return remoteconnection;
	}

	public void setRemoteconnection(String remoteconnection) {
		this.remoteconnection = remoteconnection;
	}
}