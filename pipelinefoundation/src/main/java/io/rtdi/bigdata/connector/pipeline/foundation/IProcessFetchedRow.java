package io.rtdi.bigdata.connector.pipeline.foundation;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;

/**
 * Rows can be processed either in the binary format or in the GenericRecord format. Both calls should lead to the exact same results.
 *
 */
public interface IProcessFetchedRow {
	
	void process(String topic, long offset, int partition, byte[] key, byte[] value) throws IOException;

	void process(String topic, long offset, int partition, GenericRecord keyRecord, GenericRecord valueRecord, int keyschemaid, int valueschemaid) throws IOException;

}
