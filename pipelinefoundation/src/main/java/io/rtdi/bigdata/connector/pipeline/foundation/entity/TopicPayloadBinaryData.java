package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import com.github.benmanes.caffeine.cache.Cache;

import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineServer;
import io.rtdi.bigdata.connector.pipeline.foundation.ISchemaRegistrySource;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicPayload;

public class TopicPayloadBinaryData {

	private List<TopicPayloadBinary> rows;
	
	public TopicPayloadBinaryData() {
	}

	public TopicPayloadBinaryData(IPipelineAPI<?, ?, ?, ?> api, String topicname) throws IOException {
		List<TopicPayload> data = api.getLastRecords(topicname,20);
		if (data != null) {
			rows = new ArrayList<>();
			for (TopicPayload d : data) {
				rows.add(new TopicPayloadBinary(d));
			}
		}
	}

	public TopicPayloadBinaryData(List<TopicPayloadBinary> data) {
		this();
		this.rows = data;
	}

	public TopicPayloadBinaryData(List<TopicPayload> data, IPipelineServer<?, ?, ?, ?> api) throws IOException {
		if (data != null) {
			rows = new ArrayList<>();
			for (TopicPayload d : data) {
				rows.add(new TopicPayloadBinary(d));
			}
		}
	}

	public List<TopicPayloadBinary> getRows() {
		return rows;
	}

	public void setRows(List<TopicPayloadBinary> rows) {
		this.rows = rows;
	}

	public List<TopicPayload> asTopicPayloadList(ISchemaRegistrySource registry, Cache<Integer, Schema> schemacache) throws IOException {
		if (rows == null) {
			return null;
		} else {
			List<TopicPayload> data = new ArrayList<>(rows.size());
			for (TopicPayloadBinary r : rows) {
				data.add(new TopicPayload(r, registry, schemacache));
			}
			return data;
		}
	}

}
