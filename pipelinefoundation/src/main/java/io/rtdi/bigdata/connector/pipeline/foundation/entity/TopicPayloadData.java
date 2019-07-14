package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import java.io.IOException;
import java.util.List;

import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;

public class TopicPayloadData {

	private List<TopicPayload> rows;
	
	public TopicPayloadData() {
	}

	public TopicPayloadData(IPipelineAPI<?, ?, ?, ?> api, String topicname) throws IOException {
		rows = api.getLastRecords(topicname,20);
	}

	public TopicPayloadData(List<TopicPayload> data) {
		this();
		this.rows = data;
	}

	public List<TopicPayload> getRows() {
		return rows;
	}

	public void setRows(List<TopicPayload> rows) {
		this.rows = rows;
	}

}
