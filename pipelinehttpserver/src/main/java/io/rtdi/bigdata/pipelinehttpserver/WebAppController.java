package io.rtdi.bigdata.pipelinehttpserver;

import java.io.File;
import java.io.IOException;
import java.util.ServiceLoader;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineServer;
import io.rtdi.bigdata.connector.pipeline.foundation.PipelineServerAbstract;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

@WebListener
public class WebAppController implements ServletContextListener {

	private static final String ERRORMESSAGE = "ERRORMESSAGE";
	private static final String API = "API";
	protected final Logger logger = LogManager.getLogger(this.getClass().getName());


	public static String getError(ServletContext servletContext) {
		return (String) servletContext.getAttribute(ERRORMESSAGE);
	}

	public static PipelineServerAbstract<?, ?, ?, ?> getPipelineAPI(ServletContext servletContext) {
		return (PipelineServerAbstract<?, ?, ?, ?>) servletContext.getAttribute(API);
	}
	

	public static IPipelineServer<?, ?, ?, ?> getPipelineAPIOrFail(ServletContext servletContext) throws PipelineRuntimeException {
		IPipelineServer<?, ?, ?, ?> api = getPipelineAPI(servletContext);
		if (api == null) {
			throw new PipelineRuntimeException("Servlet does not have an API class associated");
		}
		return api;
	}


	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		PipelineServerAbstract<?, ?, ?, ?> api = getPipelineAPI(sce.getServletContext());
		if (api != null) {
			api.close(); // Just to be on the safe side
		}
	}
	
	private static void setError(ServletContextEvent sce, String errormessage) {
		sce.getServletContext().setAttribute(ERRORMESSAGE, errormessage);
	}

	@Override
	public void contextInitialized(ServletContextEvent sce) {
		try {
			IPipelineServer<?,?,?,?> api = null;
			
			@SuppressWarnings("rawtypes")
			ServiceLoader<IPipelineServer> loader = ServiceLoader.load(IPipelineServer.class);
			int count = 0;
			for (IPipelineServer<?,?,?,?> serv : loader) {
			    api = serv;
			    count++;
			}
			
			if (count == 0) {
				throw new PropertiesException("No class for an IPipelineServer was found. Seems a jar file is missing?");
			} else if (count != 1) {
				logger.error("More than one IPipelineServer class was found, hence might be using the wrong one \"{}\"", loader.toString());
				setError(sce, "More than one IPipelineServer class was found, hence might be using the wrong one");
			}
			sce.getServletContext().setAttribute(API, api);
			String webinfdirpath = sce.getServletContext().getRealPath("WEB-INF");
			File webinfdir = new File(webinfdirpath);
			api.loadConnectionProperties(webinfdir);
		} catch (IOException e) {
			logger.error("WebApp failed to start properly", e);
			setError(sce, e.getMessage());
		}
	}

}
