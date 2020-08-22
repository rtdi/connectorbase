package io.rtdi.bigdata.connectors.pipeline.kafkadirect;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.connector.pipeline.foundation.MicroServiceTransformation;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;

public class ValueMapperMicroService implements ValueTransformer<JexlRecord, JexlRecord> {

	private Map<String, List<? extends MicroServiceTransformation>> transformations;
	protected Logger logger = LogManager.getLogger(this.getClass().getName());
	@SuppressWarnings("unused")
	private ProcessorContext context;

	public ValueMapperMicroService(Map<String, List<? extends MicroServiceTransformation>> transformations) {
		this.transformations = transformations;
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
			String schemaname = value.getSchema().getName();
			List<? extends MicroServiceTransformation> steps = transformations.get(schemaname);
			if (steps != null) {
				for (MicroServiceTransformation step : steps) {
					value = step.apply(value);
				}
			}
			return value;
		} catch (IOException e) {
			logger.error("IOException thrown", e);
			throw new ProcessorStateException(e);
		}
	}

}
