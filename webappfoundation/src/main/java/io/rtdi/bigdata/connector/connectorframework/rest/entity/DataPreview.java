package io.rtdi.bigdata.connector.connectorframework.rest.entity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicPayload;

public class DataPreview {

	private List<PreviewRow> rows;
	
	public DataPreview() {
	}

	public DataPreview(IPipelineAPI<?, ?, ?, ?> api, String topicname) throws IOException {
		List<TopicPayload> data = api.getLastRecords(topicname,20);
		if (data != null) {
			rows = new ArrayList<>();
			for (TopicPayload d : data) {
				rows.add(new PreviewRow(d));
			}
		}
	}

	public DataPreview(List<PreviewRow> data) {
		this();
		this.rows = data;
	}

	public List<PreviewRow> getRows() {
		return rows;
	}

	public void setRows(List<PreviewRow> rows) {
		this.rows = rows;
	}

	
	public class PreviewRow {

		private long offset;
		private Integer partition;
		private long timestamp;
		private String valuerecordstring;
		private String keyrecordstring;
		private int keyschemaid;
		private int valueschemaid;
		private String topic;
		private String schemaname;

		public PreviewRow() {
		}

		public PreviewRow(TopicPayload d) {
			this.offset = d.getOffset();
			this.partition = d.getPartition();
			this.timestamp = d.getTimestamp();
			this.keyrecordstring = d.getKeyRecord().toString();
			this.valuerecordstring = d.getValueRecord().toString();
			this.topic = d.getTopic();
			this.keyschemaid = d.getValueSchemaId();
			this.valueschemaid = d.getValueSchemaId();
			this.schemaname = d.getValueRecord().getSchema().getName();
		}

		public long getOffset() {
			return offset;
		}

		public Integer getPartition() {
			return partition;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public String getTopic() {
			return topic;
		}
		
		public void setTopic(String topic) {
			this.topic = topic;
		}

		@Override
		public String toString() {
			return "PreviewRow (offset=" + String.valueOf(offset) + ")";
		}

		public String getValueRecordString() {
			return valuerecordstring;
		}

		public String getKeyRecordString() {
			return keyrecordstring;
		}

		public int getKeySchemaId() {
			return keyschemaid;
		}

		public int getValueSchemaId() {
			return valueschemaid;
		}

		public String getSchemaname() {
			return schemaname;
		}
	}
	
}
