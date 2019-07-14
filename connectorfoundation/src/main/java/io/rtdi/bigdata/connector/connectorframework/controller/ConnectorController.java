package io.rtdi.bigdata.connector.connectorframework.controller;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Properties;

import io.rtdi.bigdata.connector.connectorframework.IConnectorFactory;
import io.rtdi.bigdata.connector.connectorframework.entity.UsageStatistics;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorRuntimeException;
import io.rtdi.bigdata.connector.connectorframework.utils.UsageStatisticSender;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerExitType;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;

/**
 * This is the root object of the controller tree. The exact hierarchy is
 * <ul>
 * <li>
 *   ConnectorController
 *   <ul>
 *   <li>
 *     {@link ConnectionController}
 *     <ul>
 *     <li>
 *       {@link ProducerController}
 *       <ul>
 *       <li>
 *         {@link ProducerInstanceController}
 *       </li>
 *       </ul>
 *     </li>
 *     <li>
 *       {@link ConsumerController}
 *       <ul>
 *       <li>
 *         {@link ConsumerInstanceController}
 *       </li>
 *       </ul>
 *     </li>
 *     </ul>
 *   </li>
 *   </ul>
 * </li>
 * </ul>
 *
 */
public class ConnectorController extends ThreadBasedController<ConnectionController> {
	private IConnectorFactory<?, ?, ?> connectorfactory;
	private IPipelineAPI<?,?,?,?> api;
	private File configdir;
	private HashMap<String, ConnectionController> connections = new HashMap<>();
	private UsageStatistics previousstatistics = null;
	private UsageStatisticSender sender = new UsageStatisticSender();
	private Thread usagesender = null;
	private GlobalSettings globalsettings;
	
	/**
	 * @param api with the PipelineAPI instance
	 * @param factory for the connector to create all concrete classes
	 * @param propertiespath pointing to the global.properties file
	 * @param connectordirpath pointing to the root directory of the connection properties
	 */
	public ConnectorController(IPipelineAPI<?,?,?,?> api, IConnectorFactory<?, ?, ?> factory, String connectordirpath, Properties props) {
		super(factory.getConnectorName());
		this.connectorfactory = factory;
		this.api = api;
		this.configdir = new File(connectordirpath);
		globalsettings = new GlobalSettings(props);
	}
	
	/**
	 * Every hour send usage statistics to the central database
	 * 
	 * @see io.rtdi.bigdata.connector.connectorframework.controller.Controller#controllerLoop(int)
	 */
	@Override
	protected void controllerLoop(int executioncounter) {
		if (executioncounter % 3600 == 0) {
			// every hour and at start
			UsageStatistics stats = new UsageStatistics(this, previousstatistics);
			sender.setUsageData(stats);
			usagesender = new Thread(sender);
			usagesender.start();
			previousstatistics = stats;
		}
	}

	/**
	 * Read the configuration directory and create the object tree
	 * @throws PropertiesException
	 */
	public void readConfigs() throws PropertiesException {
		if (configdir.isDirectory()) {
			for ( File connectiondir : configdir.listFiles()) {
		    	if (connectiondir.isDirectory()) {
		    		ConnectionController connectioncontroller = new ConnectionController(connectiondir, this);
		    		connectioncontroller.readConfigs();
		    		connections.put(connectioncontroller.getName(), connectioncontroller);
					addChild(connectioncontroller.getName(), connectioncontroller);
		    	}
			}
		}
	}
	
	/**
	 * Write the entire tree's configuration to the connectordirpath (see constructor)
	 * @throws PropertiesException
	 */
	public void writeConfigs() throws PropertiesException {
		if (configdir.isDirectory()) {
			for (ConnectionController conn : connections.values()) {
				File conndir = new File(configdir.getAbsolutePath() + File.separatorChar + conn.getName());
				conndir.mkdir();
				conn.writeConfigs();
			}
		}
	}

	/**
	 * @return connector factory as provided to the constructor
	 */
	public IConnectorFactory<?, ?, ?> getConnectorFactory() {
		return connectorfactory;
	}

	/**
	 * Start the PipelineAPI
	 * 
	 * @see io.rtdi.bigdata.connector.connectorframework.controller.Controller#startControllerImpl()
	 */
	@Override
	protected void startThreadControllerImpl() throws IOException {
		api.open();
	}
	
	protected boolean retryPipelineTemporaryExceptions() {
		return true;
	}

	/**
	 * When the controller stops it does close the PipelineAPI and interrupts the usagestatistic sender in case it is active right this moment
	 * 
	 * @see io.rtdi.bigdata.connector.connectorframework.controller.Controller#stopControllerImpl(io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerExitType)
	 */
	@Override
	protected void stopThreadControllerImpl(ControllerExitType exittype) {
		api.close();
		if (usagesender != null && usagesender.isAlive()) {
			usagesender.interrupt();
			try {
				usagesender.join(1000);
			} catch (InterruptedException e) {
				logger.info("UsageSender Thread did not shutdown within one second when interrupted");
			}
		}
	}

	public IPipelineAPI<?, ?, ?, ?> getPipelineAPI() {
		return api;
	}

	@Override
	protected String getControllerType() {
		return "ConnectorController";
	}

	public HashMap<String, ConnectionController> getConnections() {
		return connections;
	}

	public ConnectionController getConnection(String connectionname) {
		return connections.get(connectionname);
	}

	public ConnectionController getConnectionOrFail(String connectionname) throws ConnectorCallerException {
		ConnectionController c = connections.get(connectionname);
		if (c == null) {
			throw new ConnectorCallerException("Connection with that name not found", null, connectionname);
		} else {
			return c;
		}
	}

	public File getConnectorDirectory() {
		return configdir;
	}

	public ConnectionController addConnection(ConnectionProperties props) throws ConnectorRuntimeException {
		File connectiondir = new File(configdir.getAbsolutePath() + File.separatorChar + props.getName());
		ConnectionController connectioncontroller = new ConnectionController(connectiondir, this);
		connectioncontroller.setConnectionProperties(props);
		connections.put(connectioncontroller.getName(), connectioncontroller);
		addChild(connectioncontroller.getName(), connectioncontroller);
		return connectioncontroller;
	}

	public boolean removeConnection(ConnectionController conn) throws IOException {
		connections.remove(conn.getName());
		conn.stopController(ControllerExitType.ABORT);
		conn.joinAll(ControllerExitType.ABORT);
		File connectiondir = new File(configdir.getAbsolutePath() + File.separatorChar + conn.getConnectionProperties().getName());
		Files.walk(connectiondir.toPath()).sorted(Comparator.reverseOrder()).forEach(t -> {
			try {
				Files.delete(t);
			} catch (IOException e) {
			}
		});
		return connectiondir.exists() == false;
	}
	
	public class GlobalSettings {
		private String ui5url = "https://openui5.hana.ondemand.com/resources/sap-ui-core.js";
		private String companyname = null;
		
		public GlobalSettings() {
		}

		public GlobalSettings(Properties props) {
			if (props != null) {
				if (props.getProperty("ui5url") != null) {
					ui5url = props.getProperty("ui5url");
				}
				companyname = props.getProperty("companyname");
			}
		}

		public String getUi5url() {
			return ui5url;
		}

		public void setUi5url(String ui5url) {
			this.ui5url = ui5url;
		}

		public String getCompanyName() {
			return companyname;
		}

		public void setCompanyName(String companyname) {
			this.companyname = companyname;
		}

	}

	public GlobalSettings getGlobalSettings() {
		return globalsettings;
	}
}
