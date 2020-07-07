package io.rtdi.bigdata.pipelinetest;

import java.io.File;
import java.io.IOException;

import io.rtdi.bigdata.connector.pipeline.foundation.PipelineAbstract;
import io.rtdi.bigdata.connector.pipeline.foundation.ServiceSession;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;
import io.rtdi.bigdata.connector.properties.PipelineConnectionProperties;
import io.rtdi.bigdata.connector.properties.PipelineConnectionServerProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;
import io.rtdi.bigdata.connector.properties.ServiceProperties;

public class PipelineTest extends PipelineAbstract<PipelineConnectionProperties, TopicHandlerTest, ProducerSessionTest, ConsumerSessionTest> {
	

	public PipelineTest() {
		super(new PipelineServerTest(new PipelineConnectionServerProperties("EMPTY")));
	}

	@Override
	protected ProducerSessionTest createProducerSession(ProducerProperties properties) throws PropertiesException {
		return new ProducerSessionTest(properties, this);
	}

	@Override
	protected ConsumerSessionTest createConsumerSession(ConsumerProperties properties) throws PropertiesException {
		return new ConsumerSessionTest(properties, this);
	}

	@Override
	public String getConnectionLabel() {
		return "LocalTest";
	}

	@Override
	public boolean hasConnectionProperties() {
		return true;
	}

	@Override
	public void setWEBINFDir(File webinfdir) {
	}

	@Override
	public ServiceSession createNewServiceSession(ServiceProperties<?> properties) throws PropertiesException {
		return null;
	}

	@Override
	public String getBackingServerConnectionLabel() throws IOException {
		return null;
	}

}
