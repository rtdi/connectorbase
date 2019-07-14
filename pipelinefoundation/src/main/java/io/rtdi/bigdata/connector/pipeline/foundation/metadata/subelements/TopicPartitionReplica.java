package io.rtdi.bigdata.connector.pipeline.foundation.metadata.subelements;

public class TopicPartitionReplica {
	private int broker;
	private boolean leader;
	private boolean inSync;
	
	public TopicPartitionReplica() {
	}

	public int getBroker() {
		return broker;
	}

	public void setBroker(int broker) {
		this.broker = broker;
	}

	public boolean isLeader() {
		return leader;
	}

	public void setLeader(boolean leader) {
		this.leader = leader;
	}

	public boolean isInSync() {
		return inSync;
	}

	public void setInSync(boolean inSync) {
		this.inSync = inSync;
	}

	@Override
	public String toString() {
		return "PartitionReplicaRestEntity with " + String.valueOf(broker) + " brokers, isLeader=" + String.valueOf(leader) + ", inSync=" + String.valueOf(inSync);
	}

}
