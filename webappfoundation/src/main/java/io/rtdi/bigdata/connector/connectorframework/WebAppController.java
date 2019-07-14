package io.rtdi.bigdata.connector.connectorframework;

import java.io.File;
import java.io.InputStream;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.ServiceLoader;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.connector.connectorframework.IConnectorFactory;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
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
	private static final String API = "API";
	private static final String ERRORMESSAGE = "ERRORMESSAGE";
	private ConnectorController connectorcontroller = null;
	protected final Logger logger = LogManager.getLogger(this.getClass().getName());

	
	/**
	 * @param servletContext
	 * @return The root ConnectorController with all its connections, producers and consumers
	 */
	public static ConnectorController getConnector(ServletContext servletContext) {
		return (ConnectorController) servletContext.getAttribute(CONNECTOR);
	}

	/**
	 * @param servletContext
	 * @return The PipelineAPI to interact with the pipeline server
	 */
	public static IPipelineAPI<?,?,?,?> getPipelineAPI(ServletContext servletContext) {
		return (IPipelineAPI<?,?,?,?>) servletContext.getAttribute(API);
	}

	/**
	 * @param servletContext
	 * @return The ConnectorFactory to check if the Connector's class was found
	 */
	public static IConnectorFactory<?, ?, ?> getConnectorFactory(ServletContext servletContext) {
		return (IConnectorFactory<?, ?, ?>) servletContext.getAttribute(CONNECTORFACTORY);
	}

	/**
	 * @param servletContext
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
	 * @param servletContext
	 * @return Error string created during boot of the web application
	 */
	public static String getError(ServletContext servletContext) {
		return (String) servletContext.getAttribute(ERRORMESSAGE);
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
	
	/**
	 * @param sce ServletContextEvent
	 * @param errormessage of the Exception
	 */
	private static void setError(ServletContextEvent sce, String errormessage) {
		sce.getServletContext().setAttribute(ERRORMESSAGE, errormessage);
	}

	private static void setError(ServletContextEvent sce, Exception e) {
		sce.getServletContext().setAttribute(ERRORMESSAGE, e.getMessage());
	}

	/**
	 * At boot time the server tries to find the pipeline and connector classes, reads the 
	 * properties and starts up everything.<br/>
	 * This might fail for many reasons, e.g. the global.properties file has not been created yet etc.
	 * In all these cases the web application should start still so a UI to see the error and configure all
	 * can be presented.<br/>
	 * The main consumer of this information is the {@link io.rtdi.bigdata.connector.connectorframework.servlet.IndexPage IndexPage} 
	 * as this redirects the browser to the appropriate detail page or shows the error itself.
	 *  
	 * @see javax.servlet.ServletContextListener#contextInitialized(javax.servlet.ServletContextEvent)
	 */
	@Override
	public void contextInitialized(ServletContextEvent sce) {
        try {
			IPipelineAPI<?,?,?,?> api = null;
			String apiclassname = null;
			String webinfdirpath = sce.getServletContext().getRealPath("WEB-INF");
			File webinfdir = new File(webinfdirpath);

			// First step is the global properties file. It might contain optional information.
			Properties globalprops = null;
			try (InputStream propertiesstream = sce.getServletContext().getResourceAsStream("WEB-INF/global.properties");) {
				if (propertiesstream != null) {
					globalprops = new Properties();
					globalprops.load(propertiesstream);
					apiclassname = globalprops.getProperty("api"); // optional
				}
			}
			
			// Second load the PipelineAPI
			@SuppressWarnings("rawtypes")
			ServiceLoader<IPipelineAPI> loader = ServiceLoader.load(IPipelineAPI.class);
			int count = 0;
			for (IPipelineAPI<?,?,?,?> serv : loader) {
			    api = serv;
				if (apiclassname != null && apiclassname.equals(serv.getClass().getSimpleName())) {
					count = 1;
					break;
				}
			    count++;
			}
			
			if (count == 0) {
				throw new PropertiesException("No class for an IPipelineServer was found. Seems a jar file is missing?");
			} else if (count != 1) {
				logger.error("More than one IPipelineAPI class was found, hence might be using the wrong one");
				setError(sce, "More than one IPipelineAPI class was found, hence might be using the wrong one");
			}
			sce.getServletContext().setAttribute(API, api);

			
			// Third load the connector
			IConnectorFactory<?, ?, ?> connectorfactory = null;
			@SuppressWarnings("rawtypes")
			ServiceLoader<IConnectorFactory> connectorloader = ServiceLoader.load(IConnectorFactory.class);
			count = 0;
			for (IConnectorFactory<?,?,?> serv : connectorloader) {
				connectorfactory = serv;
			    count++;
			}
			if (count == 0) {
				throw new PropertiesException("No class for an IConnectorFactory was found. Seems a jar file is missing?");
			} else if (count != 1) {
				logger.error("More than one IConnectorFactory class was found, the build of the jar file is wrong");
				setError(sce, "More than one IConnectorFactory class was found, the build of the jar file is wrong");
			}

			sce.getServletContext().setAttribute(CONNECTORFACTORY, connectorfactory);

			String connectordirpath = sce.getServletContext().getRealPath("WEB-INF/connector");

			connectorcontroller = new ConnectorController(api, connectorfactory, connectordirpath, globalprops); // globalprops can be null
			sce.getServletContext().setAttribute(CONNECTOR, connectorcontroller);

			// Fourth step is to read the properties of each and start the controller
			api.loadConnectionProperties(webinfdir);
			connectorcontroller.readConfigs();
			connectorcontroller.startController();
			
		} catch (Exception e) {
			// An error is fine, it allows to start the web application still
			logger.error(e);
			setError(sce, e);
		}
	}

}
