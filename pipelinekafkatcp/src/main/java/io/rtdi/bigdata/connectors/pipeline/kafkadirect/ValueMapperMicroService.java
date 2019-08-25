package io.rtdi.bigdata.connectors.pipeline.kafkadirect;

import java.io.IOException;

import org.apache.kafka.streams.kstream.ValueMapper;

import io.rtdi.bigdata.connector.pipeline.foundation.MicroServiceTransformation;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;

public class ValueMapperMicroService implements ValueMapper<JexlRecord, JexlRecord> {

	private MicroServiceTransformation transformation;

	public ValueMapperMicroService(MicroServiceTransformation transformation) {
		this.transformation = transformation;
	}
	
	@Override
	public JexlRecord apply(JexlRecord value) {
		try {
			return transformation.apply(value);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

}
