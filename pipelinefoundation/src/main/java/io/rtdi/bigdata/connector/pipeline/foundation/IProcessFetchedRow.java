package io.rtdi.bigdata.connector.pipeline.foundation;

import java.io.IOException;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;

public interface IProcessFetchedRow {
	
	void process(TopicName topic, long offset, int partition, JexlRecord keyRecord, JexlRecord valueRecord) throws IOException;

}
