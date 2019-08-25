package io.rtdi.bigdata.connector.pipeline.foundation;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.github.benmanes.caffeine.cache.Cache;

import io.rtdi.bigdata.connector.pipeline.foundation.entity.TopicPayloadBinary;

/**
 * The payload with similar fields as coming from Kafka. Hence for Kakfa-less implementations, the queues would actually contain objects 
 * of this type.
 *
 */
public class TopicPayload {

	private long offset;
	private Integer partition;
	private long timestamp;
	private GenericRecord valuerecord;
	private GenericRecord keyrecord;
	private int keyschemaid;
	private int valueschemaid;
	private String topic;

	public TopicPayload() {
	}

	public TopicPayload(TopicName topic, long offset, Integer partition, long timestamp, GenericRecord keyrecord, GenericRecord valuerecord, int keyschemaid, int valueschemaid) {
		this.offset = offset;
		this.partition = partition;
		this.timestamp = timestamp;
		this.keyrecord = keyrecord;
		this.valuerecord = valuerecord;
		this.topic = topic.getName();
		this.keyschemaid = keyschemaid;
		this.valueschemaid = valueschemaid;
	}

	public TopicPayload(TopicPayloadBinary r, ISchemaRegistrySource registry, Cache<Integer, Schema> schemacache) throws IOException {
		this.offset = r.getOffset();
		this.partition = r.getPartition();
		this.timestamp = r.getTimestamp();
		this.keyrecord = AvroDeserialize.deserialize(r.getKeyRecord(), registry, schemacache, null);
		this.valuerecord = AvroDeserialize.deserialize(r.getValueRecord(), registry, schemacache, null);
		this.topic = r.getTopic();
		this.keyschemaid = r.getKeySchemaId();
		this.valueschemaid = r.getValueSchemaId();
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public Integer getPartition() {
		return partition;
	}

	public void setPartition(Integer partition) {
		this.partition = partition;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public GenericRecord getValueRecord() {
		return valuerecord;
	}

	public void setValueRecord(GenericRecord valuerecord) {
		this.valuerecord = valuerecord;
	}

	public GenericRecord getKeyRecord() {
		return keyrecord;
	}

	public void setKeyRecord(GenericRecord keyrecord) {
		this.keyrecord = keyrecord;
	}

	public String getTopic() {
		return topic;
	}
	
	public void setTopic(String topic) {
		this.topic = topic;
	}

	@Override
	public String toString() {
		return "TopicPayload (offset=" + String.valueOf(offset) + ")";
	}

	public int getKeySchemaId() {
		return keyschemaid;
	}

	public void setKeySchemaId(int keyschemaid) {
		this.keyschemaid = keyschemaid;
	}

	public int getValueSchemaId() {
		return valueschemaid;
	}

	public void setValueSchemaId(int valueschemaid) {
		this.valueschemaid = valueschemaid;
	}

}
