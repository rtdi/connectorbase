package io.rtdi.bigdata.connectors.pipeline.kafkadirect;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.rtdi.bigdata.connector.pipeline.foundation.PipelineAbstract;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicPayload;

public class GetRecordsIT {
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
	public void testGetLastRecords() {
		try {
			List<TopicPayload> data = api.getLastRecords(TopicName.create("S4CHANGES"), 10);
			System.out.println("Got " + data.size() + " records");
			assertTrue(data.size() <= 10);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
