package io.rtdi.bigdata.connector.connectorframework;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.ServiceLoader;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerExitType;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesRuntimeException;

/**
 * A ServletContextListener holding the global settings and objects for the web application.
 *
 */
/**
 *
 */
@WebListener
public class WebAppController implements ServletContextListener {

	private static final String CONNECTOR = "CONNECTORCONTROLLER";
	private static final String CONNECTORFACTORY = "CONNECTORFACTORY";
	private static final String ERRORMESSAGE = "ERRORMESSAGE";
	private ConnectorController connectorcontroller = null;
	protected final Logger logger = LogManager.getLogger(this.getClass().getName());

	
	/**
	 * @param servletContext ServletContext
	 * @return The root ConnectorController with all its connections, producers and consumers
	 */
	public static ConnectorController getConnector(ServletContext servletContext) {
		return (ConnectorController) servletContext.getAttribute(CONNECTOR);
	}

	/**
	 * @param servletContext ServletContext
	 * @return The ConnectorFactory to check if the Connector's class was found
	 */
	public static IConnectorFactory<?, ?, ?> getConnectorFactory(ServletContext servletContext) {
		return (IConnectorFactory<?, ?, ?>) servletContext.getAttribute(CONNECTORFACTORY);
	}

	/**
	 * @param servletContext ServletContext
	 * @return ConnectorController; is never null
	 * @throws PropertiesRuntimeException if no ConnectorController was found at startup
	 */
	public static ConnectorController getConnectorOrFail(ServletContext servletContext) throws PropertiesRuntimeException {
		ConnectorController connector = getConnector(servletContext);
		if (connector == null) {
			throw new PropertiesRuntimeException("Server does not have an ConnectorController bound - should be impossible");
		} else {
			return connector;
		}
	}

	/**
	 * @param servletContext ServletContext
	 * @return Error string created during boot of the web application
	 */
	public static Exception getError(ServletContext servletContext) {
		return (Exception) servletContext.getAttribute(ERRORMESSAGE);
	}
	
	public static void clearError(ServletContext sc) {
		sc.setAttribute(ERRORMESSAGE, null);
	}


	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		try {
			getConnectorOrFail(sce.getServletContext()).stopController(ControllerExitType.ABORT);
			getConnectorOrFail(sce.getServletContext()).joinAll(ControllerExitType.ABORT);
		} catch (PropertiesRuntimeException e) {
			logger.error(e);
		}
		
		/*
		 * Some JDBC drivers stick to the JVM and are removed to be sure
		 */
		Enumeration<Driver> drivers = DriverManager.getDrivers();     
		while (drivers.hasMoreElements()) {
			try {
				Driver driver = drivers.nextElement();
				DriverManager.deregisterDriver(driver);
			} catch (SQLException ex) {
				logger.error(ex);
			}
		}

	}
	
	public static void setError(ServletContext sc, Exception e) {
		sc.setAttribute(ERRORMESSAGE, e);
	}

	/**
	 * At boot time the server tries to find the pipeline and connector classes, reads the 
	 * properties and starts up everything.<br>
	 * This might fail for many reasons, e.g. the global.properties file has not been created yet etc.
	 * In all these cases the web application should start still so a UI to see the error and configure all
	 * can be presented.<br>
	 * The main consumer of this information is the {@link io.rtdi.bigdata.connector.connectorframework.servlet.IndexPage IndexPage} 
	 * as this redirects the browser to the appropriate detail page or shows the error itself.
	 *  
	 * @see javax.servlet.ServletContextListener#contextInitialized(javax.servlet.ServletContextEvent)
	 */
	@Override
	public void contextInitialized(ServletContextEvent sce) {
        try {
			String configdirpath = null;
			
			try {
				InitialContext initialContext = new javax.naming.InitialContext();  
				configdirpath = (String) initialContext.lookup("java:comp/env/io.rtdi.bigdata.connectors.configpath");
				String webappname = sce.getServletContext().getServletContextName();
				if (webappname == null && configdirpath != null) { // the latter condition is not relevant as the lookup throws an exception in case it is not found
					logger.info("Tomcat context environment variable was found but the webapp has no name");
					configdirpath = null;
				} else {
					configdirpath = configdirpath + File.separatorChar + webappname;
				}
			} catch (NamingException e) {
				logger.info("Tomcat context environment variable not found", e);
			}
			
			if (configdirpath == null) {
				configdirpath = sce.getServletContext().getRealPath("WEB-INF") + File.separatorChar + "connector";
			}
			File configdir = new File(configdirpath);
			if (!configdir.exists()) {
				configdir.mkdirs();
				logger.info("The config root directory \"" + configdir.getAbsolutePath() + "\" does not exist - created it");
			}

			// First step is the global properties file. It might contain optional information.
			Properties globalprops = null;
			File propertiesfile = new File(configdirpath, "global.properties");
			if (propertiesfile.isFile()) {
				try (FileReader propertiesstream = new FileReader(propertiesfile);) {
					if (propertiesstream != null) {
						globalprops = new Properties();
						globalprops.load(propertiesstream);
					}
				} catch (IOException e) {
					logger.error("Cannot read the global properties file although it exists \"{}\"", configdirpath);
				}
			}
			
			// Third load the connector
			IConnectorFactory<?, ?, ?> connectorfactory = null;
			@SuppressWarnings("rawtypes")
			ServiceLoader<IConnectorFactory> connectorloader = ServiceLoader.load(IConnectorFactory.class);
			int count = 0;
			for (IConnectorFactory<?,?,?> serv : connectorloader) {
				connectorfactory = serv;
			    count++;
			}
			if (count == 0) {
				throw new PropertiesException("No class for an IConnectorFactory was found. Seems a jar file is missing?", 10003);
			} else if (count != 1) {
				throw new PropertiesException("More than one IConnectorFactory class was found, the build of the jar file is wrong", 10004);
			}

			sce.getServletContext().setAttribute(CONNECTORFACTORY, connectorfactory);

			logger.info("The root directory of all connector settings is \"{}\"", configdirpath);

			connectorcontroller = new ConnectorController(connectorfactory, configdirpath, globalprops); // globalprops can be null
			sce.getServletContext().setAttribute(CONNECTOR, connectorcontroller);

			// Third step is to start the controller
			connectorcontroller.readConfigs();
			connectorcontroller.startController();
			
		} catch (Exception e) {
			// An error is fine, it allows to start the web application still
			logger.error(e);
			setError(sce.getServletContext(), e);
		}
	}

}
