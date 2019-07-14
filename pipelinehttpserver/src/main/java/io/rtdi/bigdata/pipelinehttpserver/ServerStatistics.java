package io.rtdi.bigdata.pipelinehttpserver;

public class ServerStatistics {

	public int gettopics;
	public int newtopic;
	public int gettopic;
	public int getschema;
	public int registerschema;
	public int getschemas;
	public int datapreview;
	public int consumerrows;
	public int consumersession;
	public int consumercommit;
	public int producercommit;
	public int producerrows;
	public int producersession;

	public ServerStatistics() {
	}

	public synchronized void incGetTopics() {
		gettopics++;
	}

	public synchronized void incNewTopic() {
		newtopic++;
	}

	public synchronized void incGetTopic() {
		gettopic++;
	}

	public synchronized void incGetSchema() {
		getschema++;
	}

	public synchronized void incRegisterSchema() {
		registerschema++;
	}

	public synchronized void incGetSchemas() {
		getschemas++;
	}

	public synchronized void incDataPreview() {
		datapreview++;
	}

	public synchronized void incRowsConsumed(int rowcount) {
		consumerrows += rowcount;
	}

	public synchronized void incConsumerSession() {
		consumersession++;
	}

	public synchronized void incConsumerCommit() {
		consumercommit++;
	}

	public synchronized void incProducerCommit() {
		producercommit++;
	}

	public synchronized void incProducerRecord() {
		producerrows++;
	}

	public synchronized void incProducerSession() {
		producersession++;
	}

}
