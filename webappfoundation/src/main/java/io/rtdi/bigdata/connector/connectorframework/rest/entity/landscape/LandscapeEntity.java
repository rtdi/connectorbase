package io.rtdi.bigdata.connector.connectorframework.rest.entity.landscape;

import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ServiceEntity;
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

	public void addServiceNode(ServiceEntity service) throws PropertiesRuntimeException {
		String apiconnection = service.getApiconnection();
		String apiconnectionid = "A:" + apiconnection; 
		String name = service.getServiceName();
		if (service.getProducedTopicList() == null || service.getProducedTopicList().size() == 0) {
			String backingserver = service.getHostname();
			String backingserverid = "A:" + backingserver;
			addNode(apiconnectionid, NetworkNodeType.PIPELINESERVER, apiconnection, apiconnection);
			addNode(backingserverid, NetworkNodeType.PIPELINESERVER, backingserver, backingserver);
			addEdge(apiconnectionid, backingserverid);
			addEdge(backingserverid, apiconnectionid);		
		} else {
			String id = "S:" + name;
			String host = service.getHostname();
			addNode(apiconnectionid, NetworkNodeType.PIPELINESERVER, apiconnection, apiconnection);
			addNode(id, NetworkNodeType.SERVICE, name, "Running at: " + host);
			addEdge(apiconnectionid, id);
			addEdge(id, apiconnectionid);
		}
	}

	/**
	 * In rare cases a server is connected to a server. Most common example: The pipelinehttp points to the
	 * pipelinehttpserver which in turn is using one of the pipeline api connections. <br>
	 * In this case the server to server communication should be rendered in the landscape if others, e.g. a server, connects to the
	 * backing server directly. 
	 * 
	 * @param apiconnection the label of the APIConnection, e.g. the http endpoint of the pipelinehttp
	 * @param backingserverconnection the label of the server's API connection, e.g. the kafka port 
	 * @throws PropertiesRuntimeException in case the node cannot be added
	 */
	public void addBackingServer(String apiconnection, String backingserverconnection) throws PropertiesRuntimeException {
		if (backingserverconnection != null) {
			String apiconnectionid = "A:" + apiconnection; 
			String backingserverconnectionid = "A:" + backingserverconnection; 
			addNode(apiconnectionid, NetworkNodeType.PIPELINESERVER, apiconnection, apiconnection);
			addNode(backingserverconnectionid, NetworkNodeType.PIPELINESERVER, backingserverconnection, backingserverconnection);
			addEdge(apiconnectionid, backingserverconnectionid);
		}
	}

}
