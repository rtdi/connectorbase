package io.rtdi.bigdata.connector.connectorframework.rest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

import jakarta.annotation.security.RolesAllowed;
import jakarta.servlet.ServletContext;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Configuration;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.config.Configurator;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;

@Path("/")
public class LoggingService {

	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;

	@GET
	@Path("/logginglevel")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
	public Response getLogLevel() {
		try {
			Level level = LogManager.getRootLogger().getLevel();
			return Response.ok(new LoggingLevel(level)).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@POST
	@Path("/logginglevel")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
	public Response setLogLevel(LoggingLevel levelobj) {
		try {
			Level level = levelobj.value();
			Configurator.setAllLevels(LogManager.getRootLogger().getName(), level);
			return JAXBSuccessResponseBuilder.getJAXBResponse(level.name());
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/logginglevel/{levelname}")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
	public Response setLogLevel2(@PathParam("levelname") String levelname) {
		try {
			try {
				Level level = Level.valueOf(levelname);
				Configurator.setAllLevels(LogManager.getRootLogger().getName(), level);
				return Response.ok(level.name()).build();
				
			} catch (IllegalArgumentException e) {
				throw new ConnectorCallerException("Cannot set the logging level", e, "Use allowed levelnames: \"" + Level.values() + "\"", null);
			}
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/debuginfo")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
	public Response getDebugInfo() {
		try {
			Logger logger = LogManager.getLogger(this.getClass().getName());
			org.apache.logging.log4j.core.Logger loggerImpl = (org.apache.logging.log4j.core.Logger) logger;
			Map<String, Appender> appenders = loggerImpl.getAppenders();
			Appender appender = appenders.get("LOGFILE");
			File webapplog = null;
			File weblogdir;
			if (appender != null) {
				String filename = ((RollingFileAppender) appender).getFileName();
				webapplog = new File(filename);
				weblogdir = webapplog.getParentFile();
			} else {
				java.nio.file.Path logpath = java.nio.file.Path.of(servletContext.getRealPath("WEB-INF"), "..", "..", "..", "logs");
				weblogdir = logpath.toFile();
			}
			Level level = LogManager.getRootLogger().getLevel();
			String webappname = servletContext.getServletContextName();
			return Response.ok(new DebugInfo(level, webapplog, weblogdir, webappname)).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	public static class DebugInfo extends LoggingLevel {
		private String webserverlog;
		private String applog;
		private static LoggingLevel[] levels = { 
				new LoggingLevel(Level.OFF), 
				new LoggingLevel(Level.FATAL),
				new LoggingLevel(Level.ERROR),
				new LoggingLevel(Level.WARN),
				new LoggingLevel(Level.INFO),
				new LoggingLevel(Level.DEBUG),
				new LoggingLevel(Level.TRACE),
				new LoggingLevel(Level.ALL)};
		
		public DebugInfo(Level level, File webapplog, File logdir, String webappname) {
			super(level);
			File[] logfiles = logdir.listFiles();
			Arrays.sort(logfiles, Comparator.comparingLong(File::lastModified).reversed());
			File found_server_file = null;
			for (File f : logfiles) {
				if (found_server_file == null && f.getName().matches("catalina\\..*\\.log")) {
					found_server_file = f;
					break;
				}
			}
			if (found_server_file != null) {
				webserverlog = getLastLines(found_server_file);
			}
			if (webapplog != null) {
				applog = getLastLines(webapplog);
			}
			
		}
		
		private String getLastLines(File file) {
			long length = file.length();
			if (length != 0) {
				long startpoint = length - 20000L;
				try (BufferedReader r = new BufferedReader(new FileReader(file));) {
					if (startpoint > 0) {
						// Move the file pointer ahead to the last 30k chars and read the complete line. That is the starting point
						r.skip(startpoint);
						r.readLine();
					}
					try (StringWriter out = new StringWriter();) {
						String line;
						while ((line = r.readLine()) != null) {
							out.append(line);
							out.append("\r\n");
						}
						return out.toString();
					}
				} catch (IOException e) {
					return null;
				}
			}
			return null;
		}

		public String getApplog() {
			return applog;
		}
		public String getWebserverlog() {
			return webserverlog;
		}
		
		public LoggingLevel[] getLogginglevels() {
			return levels;
		}

	}

	public static class LoggingLevel {
		private Level logginglevel;

		public LoggingLevel() {
			super();
		}

		public Level value() {
			return logginglevel;
		}

		public LoggingLevel(Level level) {
			super();
			this.logginglevel = level;
		}

		public String getLogginglevel() {
			return logginglevel.name();
		}

		public void setLogginglevel(String logginglevel) throws ConnectorCallerException {
			try {
				this.logginglevel = Level.valueOf(logginglevel);
			} catch (IllegalArgumentException e) {
				throw new ConnectorCallerException("Cannot set the logging level", e, "Use allowed levelnames: \"" + Level.values() + "\"", null);
			}
		}
		
	}
}
