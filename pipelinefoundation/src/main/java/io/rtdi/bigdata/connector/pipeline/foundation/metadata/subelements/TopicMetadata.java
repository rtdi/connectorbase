package io.rtdi.bigdata.connector.pipeline.foundation.metadata.subelements;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopicMetadata {

    private Map<String, String> configs;
    private List<TopicMetadataPartition> partitionRestEntities;
	private int partitioncount = -1;
	private int replicationfactor = -1;

    public TopicMetadata() {
    }

	public TopicMetadata(int partitions, int replicationfactor) {
		this();
		this.partitioncount = partitions;
		this.replicationfactor = replicationfactor;
		configs = null;
		partitionRestEntities = null;
	}

	public Map<String, String> getConfigs() {
		return configs;
	}

	public void setConfigs(Map<String, String> configs) {
		this.configs = configs;
	}

	public List<TopicMetadataPartition> getPartitions() {
		return partitionRestEntities;
	}

	public void setPartitions(List<TopicMetadataPartition> partitionRestEntities) {
		this.partitionRestEntities = partitionRestEntities;
		if (partitionRestEntities != null) {
			this.partitioncount = partitionRestEntities.size();
		} else {
			this.partitioncount = 1;
		}
	}
	
	@Override
	public String toString() {
		StringBuffer b = new StringBuffer();
		b.append("Kafka topic Properties: ");
		b.append(configs);
		b.append(", partitionRestEntities: [");
		for (int i=0; i<partitionRestEntities.size(); i++) {
			if (i != 0) {
				b.append(", ");
			}
			b.append(partitionRestEntities.get(i).toString());
		}
		b.append("]");
		return b.toString();
	}

	public int getPartitionCount() {
		return partitioncount;
	}

	public int getReplicationFactor() {
		return replicationfactor;
	}

	public void setPartitionCount(int partitioncount) {
		this.partitioncount = partitioncount;
	}

	public void setReplicationFactor(int replicationfactor) {
		this.replicationfactor = replicationfactor;
	}
	
	public void setPermanent() {
		if (configs == null) {
			configs = new HashMap<String, String>();
		}
		configs.put("cleanup.policy", "compact");
	}

}