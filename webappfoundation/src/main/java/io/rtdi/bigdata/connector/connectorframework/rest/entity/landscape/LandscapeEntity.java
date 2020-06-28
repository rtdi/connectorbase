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
		NetworkNode n = addNode(apiconnectionid, NetworkNodeType.PIPELINESERVER, apiconnection, apiconnection);
		n = addNode(producerid, NetworkNodeType.PRODUCER, producername, "Running at: " + host);
		n.addLink(apiconnectionid);
		n = addNode(remoteconnectionid, NetworkNodeType.REMOTECONNECTION, remoteconnection, "Remote system: " + remoteconnection);
		n.addLink(producerid);
	}

	public void addConsumerNode(ConsumerEntity consumer) throws PropertiesRuntimeException {
		String apiconnection = consumer.getApiconnection();
		String apiconnectionid = "A:" + apiconnection; 
		String consumername = consumer.getConsumerName();
		String consumerid = "C:" + consumername;
		String host = consumer.getHostname();
		String remoteconnection = consumer.getRemoteconnection();
		String remoteconnectionid = "R:" + remoteconnection;
		NetworkNode n = addNode(apiconnectionid, NetworkNodeType.PIPELINESERVER, apiconnection, apiconnection);
		n.addLink(consumerid);
		n = addNode(consumerid, NetworkNodeType.CONSUMER, consumername, "Running at: " + host);
		n.addLink(remoteconnectionid);
		n = addNode(remoteconnectionid, NetworkNodeType.REMOTECONNECTION, remoteconnection, "Remote system: " + remoteconnection);
	}

	public void addServiceNode(ServiceEntity service) throws PropertiesRuntimeException {
		String apiconnection = service.getApiconnection();
		String apiconnectionid = "A:" + apiconnection; 
		String name = service.getServiceName();
		if (service.getProducedTopicList() == null || service.getProducedTopicList().size() == 0) {
			String backingserver = service.getHostname();
			String backingserverid = "A:" + backingserver;
			NetworkNode n = addNode(apiconnectionid, NetworkNodeType.PIPELINESERVER, apiconnection, apiconnection);
			n.addLink(backingserverid);
			n = addNode(backingserverid, NetworkNodeType.PIPELINESERVER, backingserver, backingserver);
			n.addLink(apiconnectionid);
		} else {
			String id = "S:" + name;
			String host = service.getHostname();
			NetworkNode n = addNode(apiconnectionid, NetworkNodeType.PIPELINESERVER, apiconnection, apiconnection);
			n.addLink(id);
			n = addNode(id, NetworkNodeType.SERVICE, name, "Running at: " + host);
			n.addLink(apiconnectionid);
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
			NetworkNode n = addNode(apiconnectionid, NetworkNodeType.PIPELINESERVER, apiconnection, apiconnection);
			n.addLink(backingserverconnectionid);
			n = addNode(backingserverconnectionid, NetworkNodeType.PIPELINESERVER, backingserverconnection, backingserverconnection);
		}
	}

}
