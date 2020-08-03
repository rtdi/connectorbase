package io.rtdi.bigdata.connector.pipeline.foundation.test;

import java.io.IOException;

import io.rtdi.bigdata.connector.pipeline.foundation.IProcessFetchedRow;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;

public class FetchRowProcessor implements IProcessFetchedRow {

	public FetchRowProcessor() {
	}

	@Override
	public void process(String topic, long offset, int partition, JexlRecord keyRecord, JexlRecord valueRecord) throws IOException {
		System.out.println("Process row for topic " + topic + " with offset " + offset);
	}

}
