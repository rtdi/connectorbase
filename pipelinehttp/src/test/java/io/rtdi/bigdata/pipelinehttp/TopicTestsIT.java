package io.rtdi.bigdata.pipelinehttp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.avro.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.rtdi.bigdata.connector.pipeline.foundation.IProcessFetchedRow;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaRegistryName;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroInt;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroLong;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroString;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroTimestamp;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RowType;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.AvroRecordArray;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.KeySchema;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.ValueSchema;
import io.rtdi.bigdata.connector.pipeline.foundation.test.FetchRowProcessor;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;

public class TopicTestsIT {
	private PipelineHttp api;

	@Before
	public void setUp() throws Exception {
		ConnectionPropertiesHttp connprops = new ConnectionPropertiesHttp();
		connprops.setAdapterServerURI("https://localhost/pipelinehttpserver");
		connprops.setUser("pipeline1");
		connprops.setPassword("pipeline1");
		api = new PipelineHttp(connprops);
		api.open();
	}

	@After
	public void tearDown() throws Exception {
		if (api != null) {
			api.close();
		}
	}

	@Test
	public void test() {
		try {
			TopicName t = TopicName.create("topic1");
			TopicHandlerHttp handlerin = api.getTopicOrCreate(t, 1, (short) 1);
			TopicHandlerHttp handlerout = api.getTopic(t);
			assertEquals(handlerin, handlerout);
			
			List<TopicName> list = api.getTopics();
			assertTrue(list != null && list.size() > 0);
			
			SchemaHandler schema = api.registerSchema(SchemaRegistryName.create("HWMonitor"), null, getKeySchema(), getValueSchema());
			assertNotNull(schema);
			
			JexlRecord keyrecord = new JexlRecord(schema.getKeySchema());
			keyrecord.put("HOST", "localhost");
			JexlRecord valuerecord = new JexlRecord(schema.getValueSchema());
			valuerecord.put("HOST", "localhost");
			valuerecord.put("DURATION", 50);

			
			ProducerSessionHttp producersession = api.createNewProducerSession(new ProducerProperties("default"));
			
			
			producersession.open();
			producersession.beginDeltaTransaction("x", 0);
			for (int i = 0; i < 2; i++) {
				keyrecord.put("TIMESTAMP", System.currentTimeMillis());
				valuerecord.put("TIMESTAMP", keyrecord.get("TIMESTAMP"));
	
				producersession.addRow(handlerout, null, schema, keyrecord, valuerecord, RowType.INSERT, null, null);
			}
			producersession.commitDeltaTransaction();
			
			producersession.close();
			ConsumerProperties consumerprops = new ConsumerProperties("default");
			consumerprops.setTopicPattern("topic1");
			ConsumerSessionHttp consumersession = api.createNewConsumerSession(consumerprops);
			
			consumersession.open();
			IProcessFetchedRow processor = new FetchRowProcessor();
			int rows = consumersession.fetchBatch(processor);
			System.out.println("Fetched " + rows + " rows");
			consumersession.commit();
			rows = consumersession.fetchBatch(processor);
			System.out.println("Fetched " + rows + " rows");
			consumersession.close();
			
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	
	private Schema getValueSchema() throws SchemaException {
		ValueSchema valuebuilder = new ValueSchema("HWMonitor", "HWMonitor structure contains detailed data about the hardware utilization");
		valuebuilder.add("HOST", AvroString.getSchema(), "The ip address of the computer being monitored", false).setPrimaryKey();
		valuebuilder.add("TIMESTAMP", AvroTimestamp.getSchema(), "The timestamp (Unix epoch format) of this event", false).setPrimaryKey();
		valuebuilder.add("DURATION", AvroInt.getSchema(), "The duration for all relative values like CPU_USER", true);
		valuebuilder.add("CPU_USER", AvroLong.getSchema(), "Overall CPU time spent in user mode within the given duration [see Linux /proc/stat]", true);
		valuebuilder.add("CPU_SYSTEM", AvroLong.getSchema(), "Overall CPU time spent in system mode within the given duration [see Linux /proc/stat]", true);
		valuebuilder.add("CPU_IDLE", AvroLong.getSchema(), "Overall CPU time spent idle within the given duration [see Linux /proc/stat]", true);
		valuebuilder.add("CPU_WAIT_IO", AvroLong.getSchema(), "Overall CPU time spent waiting for I/O to complete within the given duration [see Linux /proc/stat]", true);
		valuebuilder.add("PROCESSES_COUNT", AvroInt.getSchema(), "Overall number of processes [see Linux /proc/stat]", true);
		valuebuilder.add("PROCESSES_RUNNING", AvroInt.getSchema(), "Nnumber of running processes [see Linux /proc/stat]", true);
		valuebuilder.add("PROCESSES_BLOCKED", AvroInt.getSchema(), "Number of processes waiting for I/O to complete [see Linux /proc/stat]", true);
		AvroRecordArray cpucorefield = valuebuilder.addColumnRecordArray("CPU_CORES", "Data about the individual cores", "CPU_CORES", "Data per CPU core [see Linux /proc/stat]");
		cpucorefield.add("CPU_ID", AvroInt.getSchema(), "The individual CPU id [see Linux /proc/stat]", true);
		cpucorefield.add("CPU_USER", AvroLong.getSchema(), "Overall CPU time spent in user mode within the given duration [see Linux /proc/stat]", true);
		cpucorefield.add("CPU_SYSTEM", AvroLong.getSchema(), "Overall CPU time spent in system mode within the given duration [see Linux /proc/stat]", true);
		cpucorefield.add("CPU_IDLE", AvroLong.getSchema(), "Overall CPU time spent idle within the given duration [see Linux /proc/stat]", true);
		cpucorefield.add("CPU_WAIT_IO", AvroLong.getSchema(), "Overall CPU time spent waiting for I/O to complete within the given duration [see Linux /proc/stat]", true);
		valuebuilder.build();
		return valuebuilder.getSchema();
	}

	private Schema getKeySchema() throws SchemaException {
		KeySchema valuebuilder = new KeySchema("HWMonitor", "HWMonitor structure contains detailed data about the hardware utilization");
		valuebuilder.add("HOST", AvroString.getSchema(), "The ip address of the computer being monitored", false);
		valuebuilder.add("TIMESTAMP", AvroLong.getSchema(), "The timestamp (Unix epoch format) of this event", false);
		valuebuilder.build();
		return valuebuilder.getSchema();
	}

}
