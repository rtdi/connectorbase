package io.rtdi.bigdata.connectors.pipeline.kafkadirect;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.KafkaProducer;

import com.github.benmanes.caffeine.cache.Cache;

import io.rtdi.bigdata.connector.pipeline.foundation.PipelineAbstract;
import io.rtdi.bigdata.connector.pipeline.foundation.ServiceSession;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;
import io.rtdi.bigdata.connector.properties.ServiceProperties;

public class KafkaAPIdirect extends PipelineAbstract<KafkaConnectionProperties, TopicHandler, ProducerSessionKafkaDirect, ConsumerSessionKafkaDirect> {

	public KafkaAPIdirect(KafkaServer server) {
		super(server);
	}

	public KafkaAPIdirect(KafkaConnectionProperties props) throws PropertiesException {
		super(new KafkaServer(props));
	}

	public KafkaAPIdirect() throws PropertiesException {
		super(new KafkaServer(new KafkaConnectionProperties()));
	}

	@Override
	protected ProducerSessionKafkaDirect createProducerSession(ProducerProperties properties) throws PropertiesException {
		return new ProducerSessionKafkaDirect(properties, this);
	}

	@Override
	protected ConsumerSessionKafkaDirect createConsumerSession(ConsumerProperties properties) throws PropertiesException {
		return new ConsumerSessionKafkaDirect(properties, getTenantID(), this);
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

	@Override
	public boolean hasConnectionProperties(File webinfdir) {
		return getAPIProperties().hasPropertiesFile(webinfdir);
	}

	@Override
	public ServiceSession createNewServiceSession(ServiceProperties<?> properties) throws PropertiesException {
		return new ServiceSessionKafkaDirect(properties, this);
	}

	public Cache<Integer, Schema> getSchemaIdCache() {
		return ((KafkaServer) getServer()).getSchemaIdCache();
	}

	@Override
	public String getBackingServerConnectionLabel() throws IOException {
		return null;
	}

}
