package io.rtdi.bigdata.connectors.pipeline.kafkadirect;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;

public class GenericAvroSerde implements Serde<JexlRecord> {

	public static final String SCHEMA_PROVIDER_CONFIG = "rtdi.io.schemaprovider";
	private final Serde<JexlRecord> inner;

	public GenericAvroSerde() {
		inner = Serdes.serdeFrom(new GenericAvroSerializer(), new GenericAvroDeserializer());
	}

	@Override
	public Serializer<JexlRecord> serializer() {
		return inner.serializer();
	}

	@Override
	public Deserializer<JexlRecord> deserializer() {
		return inner.deserializer();
	}

	@Override
	public void configure(final Map<String, ?> serdeConfig, final boolean isSerdeForRecordKeys) {
		inner.serializer().configure(serdeConfig, isSerdeForRecordKeys);
		inner.deserializer().configure(serdeConfig, isSerdeForRecordKeys);
	}

	@Override
	public void close() {
		inner.serializer().close();
		inner.deserializer().close();
	}

}