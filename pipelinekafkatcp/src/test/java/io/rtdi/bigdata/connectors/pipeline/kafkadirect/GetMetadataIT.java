package io.rtdi.bigdata.connectors.pipeline.kafkadirect;

import static org.junit.Assert.fail;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.rtdi.bigdata.connector.pipeline.foundation.PipelineAbstract;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerMetadataEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerMetadataEntity;
import io.rtdi.bigdata.connectors.pipeline.kafkadirect.KafkaAPIdirect;
import io.rtdi.bigdata.connectors.pipeline.kafkadirect.KafkaConnectionProperties;


public class GetMetadataIT {
	private PipelineAbstract<?,?,?,?> api;

	@Before
	public void setUp() throws Exception {
		File dir = new File("./src/test/resources/tmp");
		KafkaConnectionProperties kafkaprops = new KafkaConnectionProperties();
		kafkaprops.read(dir);
		api = new KafkaAPIdirect(kafkaprops);
		api.open();
	}

	@After
	public void tearDown() throws Exception {
		api.close();
	}

	@Test
	public void test() {
		try {
			ProducerMetadataEntity producertopics = api.getProducerMetadata();
			ConsumerMetadataEntity consumertopics = api.getConsumerMetadata();
			System.out.println(producertopics.getProducerList().size());
			System.out.println(consumertopics.getConsumerList().size());
		} catch (Throwable e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
