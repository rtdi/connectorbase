package io.rtdi.bigdata.connector.pipeline.foundation.test;

import java.io.IOException;

import io.rtdi.bigdata.connector.pipeline.foundation.IProcessFetchedRow;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.OperationState;

public class FetchRowProcessor implements IProcessFetchedRow {

	public FetchRowProcessor() {
	}

	@Override
	public void process(TopicName topic, long offset, long offsettimestamp, int partition, JexlRecord keyRecord, JexlRecord valueRecord) throws IOException {
		System.out.println("Process row for topic " + topic.getName() + " with offset " + offset);
	}

	@Override
	public void setOperationState(OperationState state) {
	}

	@Override
	public boolean isActive() {
		return false;
	}

	@Override
	public void incrementRowsProcessed(long offset, long offsettimestamp) {
	}

}
