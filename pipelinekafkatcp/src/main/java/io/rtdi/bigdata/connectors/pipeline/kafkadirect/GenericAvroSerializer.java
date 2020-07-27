package io.rtdi.bigdata.connectors.pipeline.kafkadirect;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.kafka.common.serialization.Serializer;

import io.rtdi.bigdata.connector.pipeline.foundation.AvroSerializer;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;

public class GenericAvroSerializer implements Serializer<JexlRecord> {

	public GenericAvroSerializer() {
	}

	@Override
	public void configure(final Map<String, ?> config, final boolean iskey) {
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
	}
}
