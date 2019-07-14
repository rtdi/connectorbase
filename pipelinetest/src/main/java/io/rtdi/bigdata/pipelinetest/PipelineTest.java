package io.rtdi.bigdata.pipelinetest;

import io.rtdi.bigdata.connector.pipeline.foundation.PipelineAbstract;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;
import io.rtdi.bigdata.connector.properties.PipelineConnectionProperties;
import io.rtdi.bigdata.connector.properties.PipelineConnectionServerProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;

public class PipelineTest extends PipelineAbstract<PipelineConnectionProperties, TopicHandlerTest, ProducerSessionTest, ConsumerSessionTest> {
	

	public PipelineTest() {
		super(new PipelineServerTest(new PipelineConnectionServerProperties("EMPTY")));
	}

	@Override
	protected ProducerSessionTest createProducerSession(ProducerProperties properties) throws PropertiesException {
		return new ProducerSessionTest(properties, getTenantID(), this);
	}

	@Override
	protected ConsumerSessionTest createConsumerSession(ConsumerProperties properties) throws PropertiesException {
		return new ConsumerSessionTest(properties, this, getTenantID());
	}

	@Override
	public String getTenantID() throws PropertiesException {
		return "TESTTENANT";
	}

	@Override
	public String getConnectionLabel() {
		return "LocalTest";
	}

}
