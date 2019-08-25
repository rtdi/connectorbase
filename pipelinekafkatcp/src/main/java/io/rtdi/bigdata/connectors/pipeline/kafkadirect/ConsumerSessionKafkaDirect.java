package io.rtdi.bigdata.connectors.pipeline.kafkadirect;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import io.rtdi.bigdata.connector.pipeline.foundation.AvroDeserialize;
import io.rtdi.bigdata.connector.pipeline.foundation.ConsumerSession;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineBase;
import io.rtdi.bigdata.connector.pipeline.foundation.IProcessFetchedRow;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicUtil;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;


public class ConsumerSessionKafkaDirect extends ConsumerSession<TopicHandler> {
	protected KafkaConsumer<byte[], byte[]> consumer = null;

	public ConsumerSessionKafkaDirect(
			ConsumerProperties properties,
			String tenantid, 
			IPipelineBase<KafkaConnectionProperties, TopicHandler> api) throws PropertiesException {
		super(properties, tenantid, api);
		try {
	        Map<String, Object> consumerprops = new HashMap<>();
			consumerprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, api.getAPIProperties().getKafkaBootstrapServers());
			consumerprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			consumerprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
			consumerprops.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, getProperties().getFlushMaxRecords());
			consumerprops.put(ConsumerConfig.GROUP_ID_CONFIG, getProperties().getName());
			consumerprops.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
			
			consumerprops.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
			consumerprops.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
			consumer = new KafkaConsumer<byte[], byte[]>(consumerprops);
		} catch (IllegalArgumentException e) {
			throw new PropertiesException("Illegal argument when calling the Kafka Consumer", e, null);
		}
	}

	@Override
	public void open() throws PropertiesException {
		consumer.subscribe(Pattern.compile(getTenantId() + "-" + getProperties().getTopicPattern()));
	}

	@Override
	public void close() {
		try {
			if (consumer != null) {
				consumer.close();
			}
		} catch (AuthenticationException | InterruptException e) {
			logger.info("Kafka consumer close failed with exception", e);
		} finally {
			consumer = null;
		}
	}

	@Override
	public int fetchBatch(IProcessFetchedRow processor) throws IOException {
		int rowcount = 0;
		ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(10));
		Iterator<ConsumerRecord<byte[], byte[]>> recordsiterator = records.iterator();
		while (recordsiterator.hasNext()) {
			ConsumerRecord<byte[], byte[]> record = recordsiterator.next();
			GenericRecord keyrecord = null;
			int[] keyschemaid = new int[1];
			try {
				keyrecord = AvroDeserialize.deserialize(record.key(), this, null, keyschemaid);
			} catch (IOException e) {
				logger.error("Cannot deserialize data Key with offset {}, row not processed", String.valueOf(record.offset()));
			}
			int[] valueschemaid = new int[1];
			GenericRecord valuerecord = null;
			try {
				valuerecord = AvroDeserialize.deserialize(record.value(), this, null, valueschemaid);
			} catch (IOException e) {
				logger.error("Cannot deserialize data Value with offset {}, row not processed", String.valueOf(record.offset()));
			}
			if (valuerecord != null) {
				String topicname = TopicUtil.extractTopicName(record.topic());
				processor.process(topicname, record.offset(), record.partition(), keyrecord, valuerecord, keyschemaid[0], valueschemaid[0]);
				rowcount++;

				// Due to a rebalance a new topic might be subscribed to
				if (getTopic(topicname) == null) { 
					addTopic(getPipelineAPI().getTopic(new TopicName(getTenantId(), topicname)));
				}
			}
		}
		return rowcount;
	}
	
	@Override
	public void commit() throws PipelineRuntimeException {
		try {
			consumer.commitSync();
		} catch (KafkaException e) {
			throw new PipelineRuntimeException("Commit failed", e, null);
		}
	}

	@Override
	public void setTopics() throws PropertiesException {
		Set<String> topics = consumer.subscription();
		for ( String t : topics) {
			addTopic(getPipelineAPI().getTopic(new TopicName(t)));
		}
	}

}
