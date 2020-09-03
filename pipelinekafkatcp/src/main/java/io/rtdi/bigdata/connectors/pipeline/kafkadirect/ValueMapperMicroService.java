package io.rtdi.bigdata.connectors.pipeline.kafkadirect;

import java.io.IOException;
import java.util.Set;

import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.connector.pipeline.foundation.MicroServiceTransformation;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;

public class ValueMapperMicroService implements ValueTransformer<JexlRecord, JexlRecord> {

	private Set<MicroServiceTransformation> transformations;
	protected Logger logger = LogManager.getLogger(this.getClass().getName());
	@SuppressWarnings("unused")
	private ProcessorContext context;

	public ValueMapperMicroService(Set<MicroServiceTransformation> set) {
		this.transformations = set;
	}
	
	@Override
	public void init(ProcessorContext context) {
		this.context = context;
	}

	@Override
	public void close() {
	}

	@Override
	public JexlRecord transform(JexlRecord value) {
		try {
			for (MicroServiceTransformation step : transformations) {
				value = step.apply(value);
			}
			return value;
		} catch (IOException e) {
			logger.error("IOException thrown", e);
			throw new ProcessorStateException(e);
		}
	}

}
