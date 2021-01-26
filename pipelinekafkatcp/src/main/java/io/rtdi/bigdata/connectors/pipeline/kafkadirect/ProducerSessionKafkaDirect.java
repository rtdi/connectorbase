package io.rtdi.bigdata.connectors.pipeline.kafkadirect;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
	private Future<RecordMetadata> transactionfuture;
	private List<Future<RecordMetadata>> sendfutures;
	private Map<String, Map<Integer, Long>> offsettable;

	public ProducerSessionKafkaDirect(ProducerProperties properties, KafkaAPIdirect api) throws PropertiesException {
		super(properties, api);
		api.getTransactionLogSchema();
	}

	@Override
	public void beginImpl() {
		sendfutures = new LinkedList<>();
		offsettable = new HashMap<>();
	}

	@Override
	public void commitImpl() throws PipelineRuntimeException {
		// This call waits for all data to be sent, no need to check manually
		if (transactionfuture != null) {
			try {
				transactionfuture.get();
			} catch (InterruptedException | ExecutionException e) {
				throw new PipelineRuntimeException("Was not able to send all data to Kafka", e, null, null);
			}
		}
	}

	@Override
	protected void abort() {
		transactionfuture = null;
		sendfutures = null;
		offsettable = null;
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
		KafkaAPIdirect.addCustomProperties(producerprops, getPipelineAPI().getGlobalSettings().getKafkaProducerProperties(), logger);
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
		sendfutures.add(producer.send(record));
		if (sendfutures.size() > 10000) {
			collapse(false);
		}
		logger.debug("Added data to the producer queue \"{}\"", topic.getTopicName().getName());
	}
	
	private boolean collapse(boolean commit) throws PipelineRuntimeException {
		String lasttopic = null;
		Map<Integer, Long> lastparttionoffsettable = null;
		Iterator<Future<RecordMetadata>> iter = sendfutures.iterator();
		while (iter.hasNext()) {
			Future<RecordMetadata> future = iter.next();
			try {
				RecordMetadata info;
				if (commit) {
					info = future.get(30, TimeUnit.SECONDS);
				} else {
					info = future.get(10, TimeUnit.MILLISECONDS);
				}
				if (lasttopic == null || !lasttopic.equals(info.topic())) {
					lasttopic = info.topic();
					lastparttionoffsettable = offsettable.get(lasttopic);
					if (lastparttionoffsettable == null) {
						lastparttionoffsettable = new HashMap<>();
						offsettable.put(lasttopic, lastparttionoffsettable);
					}
				}
				lastparttionoffsettable.put(info.partition(), info.offset());
				iter.remove();
			} catch (TimeoutException  e) {
				if (commit) {
					throw new PipelineRuntimeException("Was not able to confirm all sent data has reached Kafka", null, null, null);
				} else {
					return false;
				}
			} catch (InterruptedException | ExecutionException e) {
				throw new PipelineRuntimeException("Was not able to confirm all sent data has reached Kafka", e, null, null);
			}
		}
		return true;
	}

	@Override
	public void confirmInitialLoad(String schemaname, int producerinstance) throws IOException {
		collapse(true);
		sendLoadStatus(schemaname, producerinstance, getCurrentTransactionRowCount(), true, offsettable);
	}

	@Override
	public void markInitialLoadStart(String schemaname, int producerinstance) throws IOException {
		sendLoadStatus(schemaname, producerinstance, null, false, null);
	}
	
	@Override
	public void confirmDeltaLoad(int producerinstance) throws IOException {
		collapse(true);
		sendLoadStatus(PipelineAbstract.ALL_SCHEMAS, producerinstance, getCurrentTransactionRowCount(), true, offsettable);
	}

	private void sendLoadStatus(String schemaname, int producerinstance, Long rowcount, boolean finished,
			Map<String, Map<Integer, Long>> offsets) throws IOException {
		transactionfuture = getPipelineAPI().sendLoadStatus(
				getProperties().getName(), schemaname, producerinstance, 
				rowcount, finished, this.getSourceTransactionIdentifier(), producer, offsets);
	}

}
