package io.rtdi.bigdata.connectors.pipeline.kafkadirect;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.kafka.common.serialization.Deserializer;

import io.rtdi.bigdata.connector.pipeline.foundation.AvroDeserialize;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;

public class GenericAvroDeserializer implements Deserializer<JexlRecord> {

	private KafkaAPIdirect schemaprovider;

	public GenericAvroDeserializer() {
	}

	@Override
	public void configure(final Map<String, ?> config, final boolean iskey) {
		schemaprovider = (KafkaAPIdirect) config.get(GenericAvroSerde.SCHEMA_PROVIDER_CONFIG);
	}

	@Override
	public JexlRecord deserialize(final String topic, final byte[] bytes) {
		try {
			return AvroDeserialize.deserialize(bytes, schemaprovider, schemaprovider.getSchemaIdCache(), null);
		} catch (IOException e) {
			throw new AvroRuntimeException(e);
		}
	}

	@Override
	public void close() {
	}

}