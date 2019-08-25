package io.rtdi.bigdata.connector.connectorframework.controller;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
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
import io.rtdi.bigdata.connector.properties.ServiceProperties;

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
public class ConnectorController extends ThreadBasedController<Controller<?>> {
	private IConnectorFactory<?, ?, ?> connectorfactory;
	private IPipelineAPI<?,?,?,?> api;
	private File configdir;
	private File connectiondir;
	private File servicedir;
	private HashMap<String, ConnectionController> connections = new HashMap<>();
	private HashMap<String, ServiceController> services = new HashMap<>();
	private UsageStatistics previousstatistics = null;
	private UsageStatisticSender sender = new UsageStatisticSender();
	private Thread usagesender = null;
	private GlobalSettings globalsettings;
	
	/**
	 * @param api with the PipelineAPI instance
	 * @param factory for the connector to create all concrete classes
	 * @param props global.properties data
	 * @param connectordirpath pointing to the root directory of the connection properties
	 */
	public ConnectorController(IPipelineAPI<?,?,?,?> api, IConnectorFactory<?, ?, ?> factory, String connectordirpath, Properties props) {
		super(factory.getConnectorName());
		this.connectorfactory = factory;
		this.api = api;
		this.configdir = new File(connectordirpath);
		this.connectiondir = new File(configdir.getAbsolutePath() + File.separatorChar + "connections");
		this.servicedir = new File(configdir.getAbsolutePath() + File.separatorChar + "services");
		globalsettings = new GlobalSettings(props);
	}
	
	/**
	 * Read the configuration directory and create the object tree
	 * @throws PropertiesException if properties are invalid
	 */
	public void readConfigs() throws PropertiesException {
		if (connectiondir.isDirectory()) {
			for ( File connectiondir : connectiondir.listFiles()) {
		    	if (connectiondir.isDirectory()) {
		    		ConnectionController connectioncontroller = new ConnectionController(connectiondir, this);
		    		connectioncontroller.readConfigs();
		    		connections.put(connectioncontroller.getName(), connectioncontroller);
					addChild(connectioncontroller.getName(), connectioncontroller);
		    	}
			}
		}
		if (servicedir.isDirectory()) {
			for ( File connectiondir : servicedir.listFiles()) {
		    	if (connectiondir.isDirectory()) {
		    		ServiceController servicecontroller = new ServiceController(connectiondir, this);
		    		servicecontroller.readConfigs();
		    		services.put(servicecontroller.getName(), servicecontroller);
					addChild(servicecontroller.getName(), servicecontroller);
		    	}
			}
		}
	}
	
	/**
	 * Write the entire tree's configuration to the connectordirpath (see constructor)
	 * @throws PropertiesException if properties are invalid
	 */
	public void writeConfigs() throws PropertiesException {
		if (connectiondir.isDirectory()) {
			for (ConnectionController conn : connections.values()) {
				File conndir = new File(connectiondir.getAbsolutePath() + File.separatorChar + conn.getName());
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
	
	@Override
	public void run() {
		/*
		 * The connector itself is responsible to open/close the pipeline, see super.run();
		 */
		try {
			while (isRunning()) {
				super.run();
				if (isRunning()) {
					try {
						Thread.sleep(60000);
					} catch (InterruptedException e) {
						lastexception = e;
						interruptedflag = true;
						return;
					}
				}
			}
		} finally {
			logger.info("Connector Controller stopped, should only happen at application shutdown");
		}
	}

	@Override
	protected void periodictask(long executioncounter) {
		if (executioncounter % 7200 == 60) { // every two hours, starting one minute after start
			updateLandscape();
		}
		if (executioncounter % 1200 == 61) { // every 20 minutes, starting one minute after start
			updateSchemaCache();
		}
		/**
		 * Every hour send usage statistics to the central database
		 * 
		 */
		if (executioncounter % 3600 == 62) {
			// every hour and at start
			UsageStatistics stats = new UsageStatistics(this, previousstatistics);
			sender.setUsageData(stats);
			usagesender = new Thread(sender);
			usagesender.start();
			previousstatistics = stats;
		}
	}

	public IPipelineAPI<?, ?, ?, ?> getPipelineAPI() {
		return api;
	}

	@Override
	protected String getControllerType() {
		return "ConnectorController";
	}

	public Map<String, ConnectionController> getConnections() {
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

	public ServiceController getService(String servicename) {
		return services.get(servicename);
	}

	public ServiceController getServiceOrFail(String servicename) throws ConnectorCallerException {
		ServiceController c = services.get(servicename);
		if (c == null) {
			throw new ConnectorCallerException("Service with that name not found", null, servicename);
		} else {
			return c;
		}
	}

	public Map<String, ServiceController> getServices() {
		return services;
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
	
	public ServiceController addService(ServiceProperties<?> props) throws ConnectorRuntimeException {
		File servicedir = new File(configdir.getAbsolutePath() + File.separatorChar + "services" + File.separatorChar + props.getName());
		ServiceController servicecontroller = new ServiceController(servicedir, this);
		servicecontroller.setServiceProperties(props);
		services.put(servicecontroller.getName(), servicecontroller);
		addChild(servicecontroller.getName(), servicecontroller);
		return servicecontroller;
	}

	public boolean removeService(ServiceController service) throws IOException {
		services.remove(service.getName());
		service.stopController(ControllerExitType.ABORT);
		service.joinAll(ControllerExitType.ABORT);
		File servicedir = new File(configdir.getAbsolutePath() + File.separatorChar + "services" + File.separatorChar + service.getServiceProperties().getName());
		Files.walk(servicedir.toPath()).sorted(Comparator.reverseOrder()).forEach(t -> {
			try {
				Files.delete(t);
			} catch (IOException e) {
			}
		});
		return servicedir.exists() == false;
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

	public int getProducerCount() {
		int count = 0;
		if (connections != null) {
			for (ConnectionController c : connections.values()) {
				count += c.getProducerCount();
			}
		}
		return count;
	}

	public int getConsumerCount() {
		int count = 0;
		if (connections != null) {
			for (ConnectionController c : connections.values()) {
				count += c.getConsumerCount();
			}
		}
		return count;
	}

	public long getRowsProcessed() {
		long count = 0;
		if (connections != null) {
			for (ConnectionController c : connections.values()) {
				count += c.getRowsProcessed();
			}
		}
		if (services != null) {
			for (ServiceController c : services.values()) {
				count += c.getRowsProcessed();
			}
		}
		return count;
	}

	public Long getLastProcessed() {
		Long last = null;
		if (connections != null) {
			for (ConnectionController c : connections.values()) {
				Long l = c.getLastProcessed();
				if (l != null) {
					if (last == null || last < l) {
						last = l;
					}
				}
			}
		}
		if (services != null) {
			for (ServiceController c : services.values()) {
				Long l = c.getLastProcessed();
				if (l != null) {
					if (last == null || last < l) {
						last = l;
					}
				}
			}
		}
		return last;
	}

	@Override
	protected void updateLandscape() {
		if (connections != null) {
			for (ConnectionController c : connections.values()) {
				c.updateLandscape();
			}
		}
		if (services != null) {
			for (ServiceController c : services.values()) {
				c.updateLandscape();
			}
		}
	}

	@Override
	protected void updateSchemaCache() {
		if (connections != null) {
			for (ConnectionController c : connections.values()) {
				c.updateSchemaCache();
			}
		}
		if (services != null) {
			for (ServiceController c : services.values()) {
				c.updateSchemaCache();
			}
		}
	}

}
