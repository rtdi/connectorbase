package io.rtdi.bigdata.connectors.pipeline.kafkadirect;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.List;

import org.apache.avro.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.rtdi.bigdata.connector.pipeline.foundation.PipelineAbstract;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaName;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicPayload;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroInt;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroString;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.AvroRecordArray;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.KeySchema;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.ValueSchema;
import io.rtdi.bigdata.connectors.pipeline.kafkadirect.KafkaAPIdirect;
import io.rtdi.bigdata.connectors.pipeline.kafkadirect.KafkaConnectionProperties;

public class APITestsIT {
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
	
	public static KeySchema getKeySchema() throws SchemaException {
		KeySchema keybuilder = new KeySchema("KeyTest", null);
		keybuilder.add("EmployeeID", AvroInt.getSchema(), null, false);
		keybuilder.build();
		return keybuilder;
	}

	public static ValueSchema getValueSchema() throws SchemaException {
		ValueSchema valuebuilder = new ValueSchema("ValueTest", null);
		valuebuilder.add("EmployeeID", AvroInt.getSchema(), null, false);
		valuebuilder.add("LastName", AvroString.getSchema(), null, true);
		AvroRecordArray addressschema = valuebuilder.addColumnRecordArray("Addresses", null, "ADDRESS", null);
		addressschema.add("City", AvroString.getSchema(), null, true);
		addressschema.add("PostCode", AvroString.getSchema(), null, true);
		
		valuebuilder.build();
		return valuebuilder;
	}

	@Test
	public void testGetSchema() {
		try {
			Schema s = api.getSchema(41);
			if (s != null) {
				System.out.println(s.toString(true));
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testGetSchemas() {
		try {
			List<String> list = api.getSchemas();
			if (list != null) {
				System.out.println(list.toString());
			}
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testSchemaCreate() {
		try {
			TopicHandler topic1 = api.getTopic("Topic1");
			if (topic1 == null) {
				topic1 = api.topicCreate("Topic1", 1, 1);
			}
			Schema keyschema = getKeySchema().getSchema();
			Schema valueschema = getValueSchema().getSchema();
			@SuppressWarnings("unused")
			SchemaHandler employeeschema = api.registerSchema(new SchemaName(api.getTenantID(), "Employee"), "The employee schema", keyschema, valueschema);
			
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	// @Test
	public void testGetLastRecords() {
		try {
			List<TopicPayload> data = api.getLastRecords("Topic1", 10);
			assertTrue(data.size() <= 10);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
