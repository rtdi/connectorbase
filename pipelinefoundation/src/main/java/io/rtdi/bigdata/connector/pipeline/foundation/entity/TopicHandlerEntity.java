package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.metadata.subelements.TopicMetadata;
import io.rtdi.bigdata.connector.pipeline.foundation.metadata.subelements.TopicMetadataPartition;

public class TopicHandlerEntity {

	private String topicname;
	private int partitioncount;
	private int replicationfactor;
	private List<ConfigPair> configlist;
    private List<TopicMetadataPartition> partitionRestEntities;

	
	public TopicHandlerEntity() {
	}

	public TopicHandlerEntity(String topicname, int partitioncount, int replicationfactor, Map<String, String> configs) {
		this.topicname = topicname;
		this.partitioncount = partitioncount;
		this.replicationfactor = replicationfactor;
		if (configs != null) {
			configlist = new ArrayList<>();
			for (String key : configs.keySet()) {
				String value = configs.get(key);
				configlist.add(new ConfigPair(key, value));
			}
		}
	}
	
	public TopicHandlerEntity(TopicHandler h) {
		this.topicname = h.getTopicName().getName();
		TopicMetadata m = h.getTopicMetadata();
		if (m != null) {
			this.partitioncount = m.getPartitionCount();
			this.replicationfactor = m.getReplicationFactor();
			Map<String, String> configs = m.getConfigs();
			if (configs != null) {
				configlist = new ArrayList<>();
				for (String key : configs.keySet()) {
					String value = configs.get(key);
					configlist.add(new ConfigPair(key, value));
				}
			}
		}
	}

	public String getTopicname() {
		return topicname;
	}

	public void setTopicname(String topicname) {
		this.topicname = topicname;
	}

	public int getPartitioncount() {
		return partitioncount;
	}

	public void setPartitioncount(int partitioncount) {
		this.partitioncount = partitioncount;
	}

	public int getReplicationfactor() {
		return replicationfactor;
	}

	public void setReplicationfactor(int replicationfactor) {
		this.replicationfactor = replicationfactor;
	}

	public List<ConfigPair> getConfiglist() {
		return configlist;
	}

	public void setConfiglist(List<ConfigPair> configlist) {
		this.configlist = configlist;
	}

	public List<TopicMetadataPartition> getPartitionRestEntities() {
		return partitionRestEntities;
	}

	public void setPartitionRestEntities(List<TopicMetadataPartition> partitionRestEntities) {
		this.partitionRestEntities = partitionRestEntities;
	}

	public static class ConfigPair {
		private String key;
		private String value;
		
		public ConfigPair() {
		}
		
		public ConfigPair(String key, String value) {
			this.key = key;
			this.value = value;
		}
		
		public String getKey() {
			return key;
		}
		public void setKey(String key) {
			this.key = key;
		}
		public String getValue() {
			return value;
		}
		public void setValue(String value) {
			this.value = value;
		}
		
	}
}
