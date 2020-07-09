package io.rtdi.bigdata.connectors.pipeline.kafkadirect;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.kafka.common.serialization.Serializer;

import io.rtdi.bigdata.connector.pipeline.foundation.AvroSerializer;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaName;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;

public class GenericAvroSerializer implements Serializer<JexlRecord> {

	/*
	 * We cache the SchemaName instead of the id because the name is stable and the id is cached in the KafkaServer itself and with a timeout
	 */
	private Map<String, SchemaName> namedirectory = new HashMap<>(); 

	public GenericAvroSerializer() {
	}

	@Override
	public void configure(final Map<String, ?> config, final boolean iskey) {
		// schemaprovider = (KafkaAPIdirect) config.get(GenericAvroSerde.SCHEMA_PROVIDER_CONFIG);
	}

	@Override
	public byte[] serialize(final String topic, final JexlRecord record) {
		try {
			return AvroSerializer.serialize(record.getSchemaId(), record);
		} catch (IOException e) {
			throw new AvroRuntimeException(e);
		}
	}

	@Override
	public void close() {
		namedirectory.clear();
	}
}
