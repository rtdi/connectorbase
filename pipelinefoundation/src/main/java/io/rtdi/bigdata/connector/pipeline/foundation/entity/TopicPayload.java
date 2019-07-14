package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import javax.xml.bind.annotation.XmlTransient;

import org.apache.avro.generic.GenericRecord;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;

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
	private String valuerecordstring;
	private String keyrecordstring;
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

	public void setValues(TopicName topic, long offset, Integer partition, long timestamp, GenericRecord keyrecord, GenericRecord valuerecord, int keyschemaid, int valueschemaid) {
		this.offset = offset;
		this.partition = partition;
		this.timestamp = timestamp;
		this.keyrecord = keyrecord;
		this.valuerecord = valuerecord;
		this.topic = topic.getName();
		this.keyschemaid = keyschemaid;
		this.valueschemaid = valueschemaid;
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

	@JsonIgnore
	@XmlTransient
	public GenericRecord getValueRecord() {
		return valuerecord;
	}

	@JsonIgnore
	public void setValueRecord(GenericRecord valuerecord) {
		this.valuerecord = valuerecord;
	}

	@JsonIgnore
	@XmlTransient
	public GenericRecord getKeyRecord() {
		return keyrecord;
	}

	@JsonIgnore
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

	public String getValueRecordString() {
		if (valuerecord != null) {
			return valuerecord.toString();
		} else {
			return valuerecordstring;
		}
	}

	public void setValueRecordString(String valuerecordstring) {
		this.valuerecordstring = valuerecordstring;
	}

	public String getKeyRecordString() {
		if (keyrecord != null) {
			return keyrecord.toString();
		} else {
			return keyrecordstring;
		}
	}

	public void setKeyRecordString(String keyrecordstring) {
		this.keyrecordstring = keyrecordstring;
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
