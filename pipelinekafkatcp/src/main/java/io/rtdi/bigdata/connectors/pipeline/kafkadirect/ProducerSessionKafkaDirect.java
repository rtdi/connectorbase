package io.rtdi.bigdata.connectors.pipeline.kafkadirect;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import io.rtdi.bigdata.connector.pipeline.foundation.AvroSerializer;
import io.rtdi.bigdata.connector.pipeline.foundation.PipelineAbstract;
import io.rtdi.bigdata.connector.pipeline.foundation.ProducerSession;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ProducerProperties;


public class ProducerSessionKafkaDirect extends ProducerSession<TopicHandler> {
	public static final long COMMIT_TIMEOUT = 20000L;
	private KafkaProducer<byte[], byte[]> producer;
	private Future<RecordMetadata> lastfuture;
	

	public ProducerSessionKafkaDirect(ProducerProperties properties, KafkaAPIdirect api) throws PropertiesException {
		super(properties, api);
		api.getTransactionLogSchema();
	}

	@Override
	public void beginImpl() {
	}

	@Override
	public void commitImpl() throws PipelineRuntimeException {
		// This call waits for all data to be sent, no need to check manually
		if (lastfuture != null) {
			try {
				lastfuture.get();
			} catch (InterruptedException | ExecutionException e) {
				throw new PipelineRuntimeException("Was not able to send all data to Kakfa", e, null, null);
			}
		}
	}

	@Override
	protected void abort() {
		lastfuture = null;
	}

	@Override
	public void open() {
        Map<String, Object> producerprops = new HashMap<>();
		producerprops.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getPipelineAPI().getAPIProperties().getKafkaBootstrapServers());
		producerprops.put(ProducerConfig.ACKS_CONFIG, "all");
		producerprops.put(ProducerConfig.RETRIES_CONFIG, 0);
		producerprops.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		producerprops.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		producerprops.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		producerprops.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		producerprops.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		getPipelineAPI().addSecurityProperties(producerprops);
		producer = new KafkaProducer<byte[], byte[]>(producerprops);
		logger.debug("Open Producer");
	}
	
	@Override
	public KafkaAPIdirect getPipelineAPI() {
		return (KafkaAPIdirect) super.getPipelineAPI();
	}

	@Override
	public void close() {
		logger.debug("Close Producer");
		producer.close(Duration.ofSeconds(20));
	}

	@Override
	protected void addRowImpl(TopicHandler topic, Integer partition, SchemaHandler handler, JexlRecord keyrecord, JexlRecord valuerecord) throws IOException {
		if (topic == null) {
			throw new PipelineRuntimeException("Sending rows requires a topic but it is null");
		} else if (keyrecord == null || valuerecord == null) {
			throw new PipelineRuntimeException("Sending rows requires a key and value record");
		}
		byte[] key = AvroSerializer.serialize(handler.getDetails().getKeySchemaID(), keyrecord);
		
		byte[] value = AvroSerializer.serialize(handler.getDetails().getValueSchemaID(), valuerecord);
		ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topic.getTopicName().getName(), partition, key, value);
		// This message might block up to max.block.ms in case the send-buffer is full
		lastfuture = producer.send(record);
		logger.debug("Added data to the producer queue \"{}\"", topic.getTopicName().getName());
	}

	@Override
	public void confirmInitialLoad(String schemaname, int producerinstance, long rowcount) throws IOException {
		sendLoadStatus(schemaname, producerinstance, rowcount, true);
	}

	@Override
	public void markInitialLoadStart(String schemaname, int producerinstance) throws IOException {
		sendLoadStatus(schemaname, producerinstance, null, false);
	}
	
	@Override
	public void confirmDeltaLoad(int producerinstance) throws IOException {
		sendLoadStatus(PipelineAbstract.ALL_SCHEMAS, producerinstance, null, true);
	}

	private void sendLoadStatus(String schemaname, int producerinstance, Long rowcount, boolean finished) throws IOException {
		lastfuture = getPipelineAPI().sendLoadStatus(
				getProperties().getName(), schemaname, producerinstance, 
				rowcount, finished, this.getSourceTransactionIdentifier(), producer);
	}

}
