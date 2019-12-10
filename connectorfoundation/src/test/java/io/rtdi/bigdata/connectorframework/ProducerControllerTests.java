package io.rtdi.bigdata.connectorframework;

import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.avro.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.rtdi.bigdata.connector.connectorframework.BrowsingService;
import io.rtdi.bigdata.connector.connectorframework.Consumer;
import io.rtdi.bigdata.connector.connectorframework.IConnectorFactory;
import io.rtdi.bigdata.connector.connectorframework.Producer;
import io.rtdi.bigdata.connector.connectorframework.Service;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConsumerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.controller.ProducerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.controller.ServiceController;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorRuntimeException;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorTemporaryException;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
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
import io.rtdi.bigdata.connector.properties.ConsumerProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;
import io.rtdi.bigdata.connector.properties.ServiceProperties;
import io.rtdi.bigdata.pipelinetest.PipelineTest;

public class ProducerControllerTests {

	private ConnectorController connector;
	private IPipelineAPI<?, ?, ?, ?> api;
	private static int messagecount = 0;

	@Before
	public void setUp() throws Exception {
		
		api = new PipelineTest();
		api.open();
		IConnectorFactory<?, ?, ?> factory = new FailingConnectorFactory();
		connector = new ConnectorController(api , factory, "src/test/resource", null);
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
			connector.startController(false);
			Thread.sleep(360000);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			System.out.println("Stopping the Connector....");
			connector.stopController(ControllerExitType.ENDBATCH);
			if (connector.joinAll(ControllerExitType.ENDBATCH)) {
				System.out.println("Stopping the Connector...done");
			} else {
				fail("Not all controller threads have been shut down");
			}
		}
	}
	
	public static class FailingConnectorFactory implements IConnectorFactory<ConnectionProperties, ProducerProperties, ConsumerProperties> {

		@Override
		public String getConnectorName() {
			return "FailingConnector";
		}

		@Override
		public Consumer<ConnectionProperties, ConsumerProperties> createConsumer(ConsumerInstanceController instance) throws IOException {
			return null;
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
		public ConsumerProperties createConsumerProperties(String name) throws PropertiesException {
			return null;
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
		public Service createService(ServiceController instance) throws PropertiesException {
			return null;
		}

		@Override
		public ServiceProperties<?> createServiceProperties(String servicename) throws PropertiesException {
			return null;
		}
		
		@Override
		public boolean supportsConnections() {
			return true;
		}

		@Override
		public boolean supportsServices() {
			return false;
		}

		@Override
		public boolean supportsProducers() {
			return true;
		}

		@Override
		public boolean supportsConsumers() {
			return false;
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
		
		public int poll(boolean after) throws IOException {
			int mod = messagecount % 10;
			if (mod == 9) {
				// in the pause interval set various errors
				mod = 10 + ((int) (messagecount/10)) % 4;
			}
			messagecount++;
			switch (mod) {
			case 0:
				System.out.println("No data");
				return 0; // for the poll to wait the polling interval once a while
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
				JexlRecord keyrecord = new JexlRecord(schemahandler.getKeySchema());
				JexlRecord valuerecord = new JexlRecord(schemahandler.getValueSchema());
				keyrecord.put("KEY", messagecount);
				valuerecord.put("KEY", messagecount);
				valuerecord.put("VALUE", 1);
				getProducerSession().addRow(topichandler, null, schemahandler,
						keyrecord, valuerecord, RowType.INSERT, String.valueOf(messagecount), "FAIL");
				System.out.println("Producing message #" + String.valueOf(messagecount));
				return 1;
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
			topichandler = getPipelineAPI().getTopicOrCreate("FAIL", 1, 1);
			schemahandler = getPipelineAPI().getSchema("FAIL");
			if (schemahandler == null) {
				schemahandler = getPipelineAPI().registerSchema("FAIL", null, getKeySchema(), getValueSchema());
			}
			addTopicSchema(topichandler, schemahandler);
		}

		@Override
		public String getLastSuccessfulSourceTransaction() throws IOException {
			return null;
		}

		@Override
		public void initialLoad() throws IOException {
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
