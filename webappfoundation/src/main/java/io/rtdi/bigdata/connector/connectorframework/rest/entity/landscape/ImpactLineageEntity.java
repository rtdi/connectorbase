package io.rtdi.bigdata.connector.connectorframework.rest.entity.landscape;

import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerEntity;
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

	public void addProducedTopic(String topicname, ProducerEntity producer) throws PropertiesRuntimeException {
		String topicid = "T:" + topicname;
		String producerid = "P:" + producer.getProducerName();
		addNode(topicid, NetworkNodeType.TOPIC, topicname, "Topic: " + topicname);
		addEdge(producerid, topicid);
	}

	public void addConsumedTopic(String topicname, ConsumerEntity consumer) throws PropertiesRuntimeException {
		String topicid = "T:" + topicname;
		String consumerid = "P:" + consumer.getConsumerName();
		addNode(topicid, NetworkNodeType.TOPIC, topicname, "Topic: " + topicname);
		addEdge(topicid, consumerid);
	}

}
