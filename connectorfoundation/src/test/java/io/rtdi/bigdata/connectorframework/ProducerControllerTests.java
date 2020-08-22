package io.rtdi.bigdata.connectorframework;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.rtdi.bigdata.connector.connectorframework.BrowsingService;
import io.rtdi.bigdata.connector.connectorframework.IConnectorFactoryProducer;
import io.rtdi.bigdata.connector.connectorframework.Producer;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.controller.ProducerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorRuntimeException;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorTemporaryException;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaRegistryName;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroInt;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerExitType;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RowType;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineTemporaryException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.KeySchema;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.ValueSchema;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;
import io.rtdi.bigdata.pipelinetest.PipelineTest;

public class ProducerControllerTests {

	private ConnectorController connector;
	private IPipelineAPI<?, ?, ?, ?> api;
	private static int messagecount = 0;

	@Before
	public void setUp() throws Exception {
		
		api = new PipelineTest();
		api.open();
		IConnectorFactoryProducer<?,?> factory = new FailingConnectorFactory();
		connector = new ConnectorController(factory, "src/test/resource", null);
		connector.setAPI(api);
		ConnectionController connection = connector.addConnection(factory.createConnectionProperties(null));
		connection.addProducer(factory.createProducerProperties(null));
		
	}

	@After
	public void tearDown() throws Exception {
		api.close();
	}

	@Test
	public void test() {
		try {
			connector.startController();
			Thread.sleep(360000);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			System.out.println("Stopping the Connector....");
			connector.stopController(ControllerExitType.ABORT);
			if (connector.joinAll(ControllerExitType.ABORT)) {
				System.out.println("Stopping the Connector...done");
			} else {
				fail("Not all controller threads have been shut down");
			}
		}
	}
	
	public static class FailingConnectorFactory implements IConnectorFactoryProducer<ConnectionProperties, ProducerProperties> {

		@Override
		public String getConnectorName() {
			return "FailingConnector";
		}

		@Override
		public Producer<ConnectionProperties, ProducerProperties> createProducer(ProducerInstanceController instance) throws IOException {
			return new FailingProducer(instance);
		}

		@Override
		public ConnectionProperties createConnectionProperties(String name) throws PropertiesException {
			return new ConnectionProperties("failingconn1");
		}

		@Override
		public ProducerProperties createProducerProperties(String name) throws PropertiesException {
			return new ProducerProperties("failingproducer1");
		}

		@Override
		public BrowsingService<ConnectionProperties> createBrowsingService(ConnectionController controller) throws IOException {
			return null;
		}

		@Override
		public boolean supportsBrowsing() {
			return false;
		}

	}

	public static class FailingProducer extends Producer<ConnectionProperties, ProducerProperties> {
		private TopicHandler topichandler;
		private SchemaHandler schemahandler;

		public FailingProducer(ProducerInstanceController instance) throws PropertiesException {
			super(instance);
		}
		
		public String poll(String transactionid) throws IOException {
			int mod = messagecount % 10;
			if (mod == 9) {
				// in the pause interval set various errors
				mod = 10 + ((int) (messagecount/10)) % 4;
			}
			messagecount++;
			switch (mod) {
			case 0:
				System.out.println("No data");
				return String.valueOf(messagecount);
			case 10:
				System.out.println("Throw a ConnectorTemporaryException");
				throw new ConnectorTemporaryException("Temp Connector error", null, null, null);
			case 11:
				System.out.println("Throw a PipelineTemporaryException");
				throw new PipelineTemporaryException("Temp Pipeline error", null, null, null);
			case 12:
				System.out.println("Throw a ConnectorRuntimeException");
				throw new ConnectorRuntimeException("Permanent Connector error", null, null, null);
			case 13:
				System.out.println("Throw a PipelineRuntimeException");
				throw new PipelineRuntimeException("Permanent Pipeline error");
			default: 
				JexlRecord valuerecord = new JexlRecord(schemahandler.getValueSchema());
				valuerecord.put("KEY", messagecount);
				valuerecord.put("VALUE", 1);
				addRow(topichandler, null, schemahandler,
						valuerecord, RowType.INSERT, String.valueOf(messagecount), "FAIL");
				System.out.println("Producing message #" + String.valueOf(messagecount));
				return String.valueOf(messagecount);
			}
		}

		@Override
		public void startProducerChangeLogging() throws IOException {
		}

		@Override
		public void startProducerCapture() throws IOException {
		}

		@Override
		public void createTopiclist() throws IOException {
			TopicName t = TopicName.create("FAIL");
			SchemaRegistryName s = SchemaRegistryName.create("FAIL");
			topichandler = getPipelineAPI().getTopicOrCreate(t, 1, (short) 1);
			schemahandler = getPipelineAPI().getSchema(s);
			if (schemahandler == null) {
				schemahandler = getPipelineAPI().registerSchema(s, null, getKeySchema(), getValueSchema());
			}
			addTopicSchema(topichandler, schemahandler);
		}

		@Override
		public void restartWith(String lastsourcetransactionid) throws IOException {
		}

		@Override
		public long getPollingInterval() {
			return 10;
		}

		@Override
		public void closeImpl() {
			System.out.println("Producer being shut down");
		}

		@Override
		protected Schema createSchema(String sourceschemaname) throws SchemaException, IOException {
			return null;
		}

		@Override
		public List<String> getAllSchemas() {
			return Collections.singletonList(schemahandler.getSchemaName().getName());
		}

		@Override
		public long executeInitialLoad(String schemaname, String transactionid) throws IOException {
			return 0L;
		}

		@Override
		public String getCurrentTransactionId() throws IOException {
			return "0";
		}

	}
	
	private static Schema getKeySchema() throws ConnectorRuntimeException {
		try {
			KeySchema keybuilder = new KeySchema("FAIL", "Dummy Schema with some payload");
			keybuilder.add("KEY", AvroInt.getSchema(), null, false);
			keybuilder.build();
			return keybuilder.getSchema();
		} catch (SchemaException e) {
			throw new ConnectorRuntimeException("SchemaException thrown", e, null, null);
		}
	}

	private static Schema getValueSchema() throws ConnectorRuntimeException {
		try {
			ValueSchema valuebuilder = new ValueSchema("FAIL", "Dummy Schema with some payload");
			valuebuilder.add("KEY", AvroInt.getSchema(), null, false);
			valuebuilder.add("VALUE", AvroInt.getSchema(), null, false);
			valuebuilder.build();
			return valuebuilder.getSchema();
		} catch (SchemaException e) {
			throw new ConnectorRuntimeException("SchemaException thrown", e, null, null);
		}
	}

}
