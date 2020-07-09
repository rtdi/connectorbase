package io.rtdi.bigdata.connectors.pipeline.kafkadirect;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
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
import io.rtdi.bigdata.connector.pipeline.foundation.ProducerSession;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ProducerProperties;


public class ProducerSessionKafkaDirect extends ProducerSession<TopicHandler> {
	public static final long COMMIT_TIMEOUT = 20000L;
	ArrayDeque<Future<RecordMetadata>> messagestatus = new ArrayDeque<Future<RecordMetadata>>();
	private KafkaProducer<byte[], byte[]> producer;

	public ProducerSessionKafkaDirect(ProducerProperties properties, KafkaAPIdirect api) throws PropertiesException {
		super(properties, api);
	}

	@Override
	public void beginImpl() {
	}

	@Override
	public void commitImpl() throws PipelineRuntimeException {
		checkMessageStatus(messagestatus);
		messagestatus.clear();
	}

	@Override
	protected void abort() {
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
	}
	
	@Override
	public KafkaAPIdirect getPipelineAPI() {
		return (KafkaAPIdirect) super.getPipelineAPI();
	}

	@Override
	public void close() {
		producer.close(Duration.ofSeconds(20));
	}

	@Override
	public void addRowBinary(TopicHandler topic, Integer partition, byte[] keyrecord, byte[] valuerecord) throws IOException {
		if (topic == null) {
			throw new PipelineRuntimeException("Sending rows requires a topic but it is null");
		} else if (keyrecord == null || valuerecord == null) {
			throw new PipelineRuntimeException("Sending rows requires a key and value record");
		}
		ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topic.getTopicName().getName(), partition, keyrecord, valuerecord);
		messagestatus.add(producer.send(record));

		throttleReceiver(messagestatus);
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
		messagestatus.add(producer.send(record));

		throttleReceiver(messagestatus);
	}

	
	
	
	/* 
	 * In case the source sends data too fast, we need to throttle it.
	 * Maximum queue size shall be 10'000 elements
	 */
	public static void throttleReceiver(ArrayDeque<Future<RecordMetadata>> messagestatus) throws PipelineRuntimeException {
		if (messagestatus.size() > 10000) {
			/*
			 * The queue is larger, so we remove all leading elements that have been sent to Kafka successfully.
			 * Multiple cases are possible:
			 * 1. Not a single message has been received by Kafka yet because the payload is so large or the network to Kafka is slow or...
			 * 2. Normal case will be that most messages were sent successfully, only the last few are pending still.
			 * 3. All messages were sent, the messagestatus queue consists of completed only.
			 * 
			 * Only in the first case the process is throttled until the oldest message is sent. In case it cannot be sent within 20 seconds,
			 * there is something wrong and the process is terminated.
			 */
			Future<RecordMetadata> firstfuture = messagestatus.getFirst(); // firstfuture cannot be null
			if (firstfuture.isDone() == false) {
				try {
					firstfuture.get(20, TimeUnit.SECONDS);
				} catch (InterruptedException | ExecutionException | TimeoutException e) {
					throw new PipelineRuntimeException("ExecutionExcpetion", e, null);
				}
			} else {
				while (firstfuture != null && firstfuture.isDone()) {
					messagestatus.removeFirst();
					if (messagestatus.size() > 0) {
						firstfuture = messagestatus.getFirst();
					} else {
						firstfuture = null;
					}
				}
			}
		}
	}

	/**
	 * This method checks if the sent data had been received by the Kafka server and confirmed. As these are asynchronous 
	 * processes potentially, the logic is to empty the queue whenever the oldest (first) message had been sent.
	 * If the overall time is longer than COMMIT_TIMEOUT, then the commit is considered failed.<BR>
	 * 
	 * The approach is not as straight forward, as there might be up to 10'000 messages in the queue. We cannot wait up to 20 seconds
	 * on each message, as then the servlet would return after hours, in case no message was sent.<BR> 
	 * 
	 * Therefore an outer loop runs for up to 20 seconds and allows each message to confirm within one second. If the first message was not
	 * sent within that time, it is tried again. This way the overall runtime of this method is between zero seconds - all messages have been 
	 * sent already - and 20+1 seconds.
	 * 
	 * @param messagestatus ArrayDeque from the Future
	 * @throws PipelineRuntimeException if error
	 */
	public static void checkMessageStatus(ArrayDeque<Future<RecordMetadata>> messagestatus) throws PipelineRuntimeException {
		if (messagestatus.size() > 0) {
			Future<RecordMetadata> firstfuture = messagestatus.getFirst();
			long endtime = System.currentTimeMillis() + COMMIT_TIMEOUT;
			
			while (firstfuture != null && System.currentTimeMillis() < endtime) {
				try {
					firstfuture.get(1, TimeUnit.SECONDS);
				} catch (TimeoutException e) {
					// that is okay
				} catch (InterruptedException | ExecutionException e) {
					throw new PipelineRuntimeException("Checking the message status failed", e, null);
				}
				/*
				 * There are multiple cases now
				 * -- The current message had been sent (isDone == true) --> remove it from the queue and test the next
				 * -- Above get did timeout as Kafka has not processed it yet --> try again
				 * -- The entire process took longer than COMMIT_TIMEOUT --> something is wrong
				 */
				if (firstfuture.isDone()) {
					messagestatus.removeFirst();
					if (messagestatus.size() > 0) {
						firstfuture = messagestatus.getFirst();
					} else {
						firstfuture = null;
					}
				}
			}
			if (firstfuture != null) {
				throw new PipelineRuntimeException("Commit did not succeed within a reasonable amount of time (\"" + String.valueOf(COMMIT_TIMEOUT) + "\")");
			}
		}
	}


}
