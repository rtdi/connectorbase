package io.rtdi.bigdata.connector.connectorframework.rest.entity.landscape;

import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesRuntimeException;

public class LandscapeEntity extends NetworkEntity {
	
	public LandscapeEntity() {
		super();
	}

	public void addProducerNode(ProducerEntity producer) throws PropertiesRuntimeException {
		String apiconnection = producer.getApiconnection();
		String apiconnectionid = "A:" + apiconnection; 
		String producername = producer.getProducerName();
		String producerid = "P:" + producername;
		String host = producer.getHostname();
		String remoteconnection = producer.getRemoteconnection();
		String remoteconnectionid = "R:" + remoteconnection;
		addNode(apiconnectionid, NetworkNodeType.PIPELINESERVER, apiconnection, apiconnection);
		addNode(producerid, NetworkNodeType.PRODUCER, producername, "Running at: " + host);
		addNode(remoteconnectionid, NetworkNodeType.REMOTECONNECTION, remoteconnection, "Remote system: " + remoteconnection);
		addEdge(producerid, apiconnectionid);
		addEdge(remoteconnectionid, producerid);
	}

	public void addConsumerNode(ConsumerEntity consumer) throws PropertiesRuntimeException {
		String apiconnection = consumer.getApiconnection();
		String apiconnectionid = "A:" + apiconnection; 
		String consumername = consumer.getConsumerName();
		String consumerid = "C:" + consumername;
		String host = consumer.getHostname();
		String remoteconnection = consumer.getRemoteconnection();
		String remoteconnectionid = "R:" + remoteconnection;
		addNode(apiconnectionid, NetworkNodeType.PIPELINESERVER, apiconnection, apiconnection);
		addNode(consumerid, NetworkNodeType.PRODUCER, consumername, "Running at: " + host);
		addNode(remoteconnectionid, NetworkNodeType.REMOTECONNECTION, remoteconnection, "Remote system: " + remoteconnection);
		addEdge(apiconnectionid, consumerid);
		addEdge(consumerid, remoteconnectionid);
	}

}
