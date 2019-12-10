package io.rtdi.bigdata.connector.pipeline.foundation.metadata.subelements;

import java.util.List;

public class TopicMetadataPartition {
	private int partition;
	private int leader;
	private List<TopicPartitionReplica> replicas;
	
	public TopicMetadataPartition() {
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public int getLeader() {
		return leader;
	}

	public void setLeader(int leader) {
		this.leader = leader;
	}

	public List<TopicPartitionReplica> getReplicas() {
		return replicas;
	}

	public void setReplicas(List<TopicPartitionReplica> replicas) {
		this.replicas = replicas;
	}

	@Override
	public String toString() {
		StringBuffer b = new StringBuffer();
		b.append("PartitionRestEntity count is ");
		b.append(partition);
		b.append(", leader count is ");
		b.append(leader);
		b.append(", PartitionReplicas: [");
		for (int i=0; i<replicas.size(); i++) {
			if (i != 0) {
				b.append(", ");
			}
			b.append(replicas.get(i).toString());
		}
		b.append("]");
		return b.toString();
	}

}
