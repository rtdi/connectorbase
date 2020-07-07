package io.rtdi.bigdata.connector.connectorframework;

import static org.junit.Assert.fail;

import org.apache.avro.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaName;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroInt;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroLong;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroString;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroTimestamp;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.AvroRecordArray;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.KeySchema;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.ValueSchema;
import io.rtdi.bigdata.pipelinetest.PipelineTest;
import io.rtdi.bigdata.pipelinetest.TopicHandlerTest;

public class LiveCycleTest {

	private PipelineTest api;

	@Before
	public void setUp() throws Exception {
		api = new PipelineTest();
	}

	@After
	public void tearDown() throws Exception {
	}

	@SuppressWarnings("unused")
	@Test
	public void test() {
		try {
			SchemaName schemaname = new SchemaName("HWMonitor");
			SchemaHandler schemahandler = api.registerSchema(schemaname, "HWMonitor structure contains detailed data about the hardware utilization",
					getKeySchema(), getValueSchema());
			TopicName topic = new TopicName("TOPIC1");
			TopicHandlerTest topichandler = api.topicCreate(topic, 1, (short) 1);
			
			/* IConnectorFactory<?, ?, ?> connector = new ConnectorFactoryTest("TEST");
			ConnectorController connectorcontroller = new ConnectorController(api, connector, "./src/test/resources/tmp", null);
			connectorcontroller.readConfigs();
			connectorcontroller.startController();
			Thread.sleep(30000);
			connectorcontroller.stopController(ControllerExitType.ENDBATCH);
			connectorcontroller.joinAll(ControllerExitType.ENDBATCH); */
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
