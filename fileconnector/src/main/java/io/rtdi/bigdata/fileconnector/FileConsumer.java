package io.rtdi.bigdata.fileconnector;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;

import io.rtdi.bigdata.connector.connectorframework.Consumer;
import io.rtdi.bigdata.connector.connectorframework.controller.ConsumerInstanceController;

public class FileConsumer extends Consumer<FileConnectionProperties, FileConsumerProperties> {

	public FileConsumer(ConsumerInstanceController instance) throws IOException {
		super(instance);
	}

	@Override
	public void process(String topic, long offset, int partition, byte[] key, byte[] value) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void process(String topic, long offset, int partition, GenericRecord keyRecord, GenericRecord valueRecord,
			int keyschemaid, int valueschemaid) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected void closeImpl() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fetchBatchStart() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void fetchBatchEnd() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void flushDataImpl() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
