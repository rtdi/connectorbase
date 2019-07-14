package io.sap.bigdata.connectors.pipeline.kafkadirect;

import org.apache.kafka.clients.producer.KafkaProducer;

import io.rtdi.bigdata.connector.pipeline.foundation.PipelineAbstract;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;

public class KafkaAPIdirect extends PipelineAbstract<KafkaConnectionProperties, TopicHandler, ProducerSessionKafkaDirect, ConsumerSessionKafkaDirect> {

	public KafkaAPIdirect(KafkaServer server) {
		super(server);
	}

	public KafkaAPIdirect(KafkaConnectionProperties props) throws PropertiesException {
		super(new KafkaServer(props));
	}

	@Override
	protected ProducerSessionKafkaDirect createProducerSession(ProducerProperties properties) throws PropertiesException {
		return new ProducerSessionKafkaDirect(properties, this);
	}

	@Override
	protected ConsumerSessionKafkaDirect createConsumerSession(ConsumerProperties properties) throws PropertiesException {
		return new ConsumerSessionKafkaDirect(properties, this);
	}
	
	public KafkaProducer<byte[], byte[]> getProducer() {
		return ((KafkaServer) getServer()).getProducer();
	}

	@Override
	public String getTenantID() throws PropertiesException {
		return getAPIProperties().getTenantid();
	}

	public KafkaServer getKafkaServer() {
		return (KafkaServer) super.getServer();
	}

	@Override
	public String getConnectionLabel() {
		return getAPIProperties().getKafkaBootstrapServers();
	}

}
