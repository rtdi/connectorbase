package io.rtdi.bigdata.connector.connectorframework.rest.entity.landscape;

import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ServiceEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesRuntimeException;

public class ImpactLineageEntity extends NetworkEntity {
	
	public ImpactLineageEntity() {
		super();
	}

	public void addProducerNode(ProducerEntity producer) throws PropertiesRuntimeException {
		String producerid = "P:" + producer.getProducerName();
		addNode(producerid, NetworkNodeType.PRODUCER, producer.getProducerName(), "Producer: " + producer.getProducerName());
	}
	
	public void addConsumerNode(ConsumerEntity consumer) throws PropertiesRuntimeException {
		String consumerid = "C:" + consumer.getConsumerName();
		addNode(consumerid, NetworkNodeType.CONSUMER, consumer.getConsumerName(), "Consumer: " + consumer.getConsumerName());
	}
	
	public void addServiceNode(ServiceEntity service) throws PropertiesRuntimeException {
		String serviceid = "S:" + service.getServiceName();
		addNode(serviceid, NetworkNodeType.SERVICE, service.getServiceName(), "Service: " + service.getServiceName());
	}


	public void addProducedTopic(String topicname, ProducerEntity producer) throws PropertiesRuntimeException {
		String topicid = "T:" + topicname;
		String producerid = "P:" + producer.getProducerName();
		NetworkNode n = addNode(topicid, NetworkNodeType.TOPIC, topicname, "Topic: " + topicname);
		n.addLink(producerid);
	}

	public void addConsumedTopic(String topicname, ConsumerEntity consumer) throws PropertiesRuntimeException {
		String topicid = "T:" + topicname;
		String consumerid = "C:" + consumer.getConsumerName();
		NetworkNode n = addNode(topicid, NetworkNodeType.TOPIC, topicname, "Topic: " + topicname);
		n.addLink(consumerid);
	}

	public void addServiceProducedTopic(String topicname, ServiceEntity service) throws PropertiesRuntimeException {
		String topicid = "T:" + topicname;
		String serviceid = "S:" + service.getServiceName();
		NetworkNode n = addNode(topicid, NetworkNodeType.TOPIC, topicname, "Topic: " + topicname);
		n.addLink(serviceid);
	}

	public void addServiceConsumedTopic(String topicname, ServiceEntity service) throws PropertiesRuntimeException {
		String topicid = "T:" + topicname;
		String serviceid = "S:" + service.getServiceName();
		NetworkNode n = addNode(topicid, NetworkNodeType.TOPIC, topicname, "Topic: " + topicname);
		n.addLink(serviceid);
	}

}
