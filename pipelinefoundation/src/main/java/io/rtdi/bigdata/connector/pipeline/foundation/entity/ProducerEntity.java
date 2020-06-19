package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.IOUtils;

public class ProducerEntity {
	private List<TopicEntity> topiclist;
	private String producername;
	private String hostname;
	private String apiconnection;
	private String remoteconnection;

	public ProducerEntity() {
	}
	
	public ProducerEntity(String name, String remoteconnection, IPipelineAPI<?,?,?,?> api, Map<TopicHandler, Set<SchemaHandler>> producermetadata) {
		this.producername = name;
		this.hostname = IOUtils.getHostname();
		this.apiconnection = api.getConnectionLabel();
		this.remoteconnection = remoteconnection;
		if (producermetadata != null) {
			topiclist = new ArrayList<>();
			for (TopicHandler topicname : producermetadata.keySet()) {
				Set<SchemaHandler> m = producermetadata.get(topicname);
				topiclist.add(new TopicEntity(topicname.getTopicName().getName(), m));
			}
		}
	}
	
	public ProducerEntity(String name, String remoteconnection, String hostname, String apilabel, Map<String, Set<String>> producermetadata) {
		this.producername = name;
		this.hostname = hostname;
		this.apiconnection = apilabel;
		this.remoteconnection = remoteconnection;
		if (producermetadata != null) {
			topiclist = new ArrayList<>();
			for (String topicname : producermetadata.keySet()) {
				Set<String> m = producermetadata.get(topicname);
				topiclist.add(new TopicEntity(topicname, m, true));
			}
		}
	}

	public List<TopicEntity> getTopicList() {
		return topiclist;
	}

	public void setTopicList(List<TopicEntity> topiclist) {
		this.topiclist = topiclist;
	}

	public String getProducerName() {
		return producername;
	}

	public void setProducerName(String producername) {
		this.producername = producername;
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

	public String getRemoteconnection() {
		return remoteconnection;
	}

	public void setRemoteconnection(String remoteconnection) {
		this.remoteconnection = remoteconnection;
	}
}