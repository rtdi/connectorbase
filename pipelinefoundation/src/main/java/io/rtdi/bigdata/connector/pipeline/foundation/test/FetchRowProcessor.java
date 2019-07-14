package io.rtdi.bigdata.connector.pipeline.foundation.test;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;

import io.rtdi.bigdata.connector.pipeline.foundation.IProcessFetchedRow;

public class FetchRowProcessor implements IProcessFetchedRow {

	public FetchRowProcessor() {
	}

	@Override
	public void process(String topic, long offset, int partition, byte[] key, byte[] value) throws IOException {
		System.out.println("Process row for topic " + topic + " with offset " + offset);
	}

	@Override
	public void process(String topic, long offset, int partition, GenericRecord keyRecord, GenericRecord valueRecord,
			int keyschemaid, int valueschemaid) throws IOException {
		System.out.println("Process row for topic " + topic + " with offset " + offset);
	}

}
