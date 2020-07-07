package io.rtdi.bigdata.pipelinetest;

import java.io.IOException;
import java.time.Duration;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.rtdi.bigdata.connector.pipeline.foundation.AvroDeserialize;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineBase;
import io.rtdi.bigdata.connector.pipeline.foundation.ProducerSession;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.connector.properties.ProducerProperties;

public class ProducerSessionTest extends ProducerSession<TopicHandlerTest> {

	private Cache<Integer, Schema> schemaidcache = Caffeine.newBuilder().expireAfterAccess(Duration.ofMinutes(30)).maximumSize(1000).build();

	public ProducerSessionTest(ProducerProperties properties, IPipelineBase<?, TopicHandlerTest> api) {
		super(properties,  api);
	}

	@Override
	public void beginImpl() throws PipelineRuntimeException {
	}

	@Override
	public void commitImpl() throws PipelineRuntimeException {
	}

	@Override
	protected void abort() throws PipelineRuntimeException {
	}

	@Override
	public void open() throws PipelineRuntimeException {
	}

	@Override
	public void close() {
	}

	@Override
	protected void addRowImpl(TopicHandlerTest topic, Integer partition, SchemaHandler handler, GenericRecord keyrecord, GenericRecord valuerecord) throws PipelineRuntimeException {
		topic.addData(keyrecord, valuerecord, handler.getDetails().getKeySchemaID(), handler.getDetails().getValueSchemaID());
	}

	@Override
	public void addRowBinary(TopicHandler topic, Integer partition, byte[] keybytes, byte[] valuebytes) throws IOException {
		TopicHandlerTest t = (TopicHandlerTest) topic;
		int[] keyschemaid = new int[1];
		int[] valueschemaid = new int[1];
		GenericRecord keyrecord = AvroDeserialize.deserialize(keybytes, this.getPipelineAPI(), schemaidcache, keyschemaid);
		GenericRecord valuerecord = AvroDeserialize.deserialize(valuebytes, this.getPipelineAPI(), schemaidcache, valueschemaid);
		t.addData(keyrecord, valuerecord, keyschemaid[0], valueschemaid[0]);
	}

}
