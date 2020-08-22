package io.rtdi.bigdata.connector.connectorframework.rest;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Comparator;

import jakarta.annotation.security.RolesAllowed;
import javax.servlet.ServletContext;
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
			java.nio.file.Path logpath = java.nio.file.Path.of(servletContext.getRealPath("WEB-INF"), "..", "..", "..", "logs");
			Level level = LogManager.getRootLogger().getLevel();
			String webappname = servletContext.getServletContextName();
			return Response.ok(new DebugInfo(level, logpath.toFile(), webappname)).build();
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
		
		public DebugInfo(Level level, File logdir, String webappname) {
			super(level);
			File[] logfiles = logdir.listFiles();
			Arrays.sort(logfiles, Comparator.comparingLong(File::lastModified).reversed());
			File found_server_file = null;
			File found_app_file = null;
			for (File f : logfiles) {
				if (found_server_file == null && f.getName().startsWith("catalina.")) {
					found_server_file = f;
				}
				if (found_app_file == null && f.getName().startsWith("webappname")) {
					found_app_file = f;
				}
				if (found_app_file != null && found_server_file != null) {
					break;
				}
			}
			if (found_server_file != null) {
				webserverlog = getLastLines(found_server_file);
			}
			if (found_app_file != null) {
				applog = getLastLines(found_app_file);
			}
			
		}
		
		private String getLastLines(File file) {
			long length = file.length();
			if (length != 0) {
				long startpoint = length - 30000L;
				try (InputStream r = new BufferedInputStream(new FileInputStream(file));) {
					r.skip(startpoint); // deals with negative numbers
					int c = r.read();
					while (c != -1 && c != '\n') {
						c = r.read(); // find next line start
					}
					byte[] data = r.readAllBytes();
					return new String(data, "UTF-8");
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
