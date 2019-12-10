package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;

import io.rtdi.bigdata.connector.pipeline.foundation.AvroSerializer;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicPayload;

/**
 * The payload with similar fields as coming from Kafka. Hence for Kakfa-less implementations, the queues would actually contain objects 
 * of this type.
 *
 */
public class TopicPayloadBinary {

	private long offset;
	private Integer partition;
	private long timestamp;
	private byte[] valuerecord;
	private byte[] keyrecord;
	private int keyschemaid;
	private int valueschemaid;
	private String topic;

	public TopicPayloadBinary() {
	}

	public TopicPayloadBinary(String topicname, long offset, Integer partition, long timestamp, GenericRecord keyrecord, GenericRecord valuerecord, int keyschemaid, int valueschemaid) throws IOException {
		this.offset = offset;
		this.partition = partition;
		this.timestamp = timestamp;
		this.keyrecord = AvroSerializer.serialize(keyschemaid, keyrecord);
		this.valuerecord = AvroSerializer.serialize(valueschemaid, valuerecord);
		this.topic = topicname;
		this.keyschemaid = keyschemaid;
		this.valueschemaid = valueschemaid;
	}

	public TopicPayloadBinary(TopicPayload data) throws IOException {
		this(data.getTopic(), data.getOffset(), data.getPartition(), data.getTimestamp(),
				data.getKeyRecord(), data.getValueRecord(), data.getKeySchemaId(), data.getValueSchemaId());
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

	public byte[] getValueRecord() {
		return valuerecord;
	}

	public void setValueRecord(byte[] valuerecord) {
		this.valuerecord = valuerecord;
	}

	public byte[] getKeyRecord() {
		return keyrecord;
	}

	public void setKeyRecord(byte[] keyrecord) {
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
