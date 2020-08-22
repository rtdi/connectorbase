package io.rtdi.bigdata.connector.connectorframework.rest;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import jakarta.annotation.security.RolesAllowed;
import javax.servlet.ServletContext;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Configuration;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ErrorEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerExitType;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;
import io.rtdi.bigdata.connector.properties.atomic.PropertyRoot;


@Path("/")
public class ConnectionService {
	
	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;
		
	@GET
	@Path("/connections")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getConnections() {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			return Response.ok(new ConnectionsEntity(connector)).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/connections/{connectionname}")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getConnectionProperties(@PathParam("connectionname") String connectionname) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnectionOrFail(connectionname);
			return Response.ok(conn.getConnectionProperties().getPropertyGroupNoPasswords()).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/connection/template")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
    public Response getConnectionPropertiesTemplate() {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			// Create an empty properties structure so the UI can show all properties needed
			return Response.ok(connector.getConnectorFactory().createConnectionProperties(null).getPropertyGroup()).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/connections/{connectionname}/stop")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_OPERATOR)
    public Response stopConnection(@PathParam("connectionname") String connectionname) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnectionOrFail(connectionname);
			conn.disableController();
			conn.joinAll(ControllerExitType.ABORT);
			return JAXBSuccessResponseBuilder.getJAXBResponse("stopped");
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/connections/{connectionname}/start")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_OPERATOR)
    public Response startConnection(@PathParam("connectionname") String connectionname) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnectionOrFail(connectionname);
			conn.startController();
			//TODO: Return if the connection was started correctly
			return JAXBSuccessResponseBuilder.getJAXBResponse("started");
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@POST
	@Path("/connections/{connectionname}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
    public Response setConnectionProperties(@PathParam("connectionname") String connectionname, PropertyRoot data) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnection(connectionname);
			if (conn == null) {
				ConnectionProperties props = connector.getConnectorFactory().createConnectionProperties(connectionname);
				props.setValue(data);
				File dir = new File(connector.getConnectorDirectory(), "connections" + File.separatorChar + connectionname);
				if (dir.exists() == false) {
					dir.mkdirs();
				}
				props.write(dir);
				connector.addConnection(props);
			} else {
				conn.getConnectionProperties().setValue(data);
				conn.getConnectionProperties().write(conn.getDirectory());
			}
			conn.stopController(ControllerExitType.ABORT);
			conn.validate();
			conn.startController();
			return JAXBSuccessResponseBuilder.getJAXBResponse("created");
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@DELETE
	@Path("/connections/{connectionname}")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
    public Response deleteConnectionProperties(@PathParam("connectionname") String connectionname) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnectionOrFail(connectionname);
			connector.removeConnection(conn);
			return JAXBSuccessResponseBuilder.getJAXBResponse("deleted");
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	public static class ConnectionsEntity {

		private List<ConnectionEntity> connections;
		
		public ConnectionsEntity(ConnectorController connector) {
			if (connector.getConnections() != null) {
				Collection<ConnectionController> connectionset = connector.getConnections().values();
				this.connections = new ArrayList<>();
				for (ConnectionController connection : connectionset) {
					this.connections.add(new ConnectionEntity(connection));
				}
			}
		}
		
		public List<ConnectionEntity> getConnections() {
			return connections;
		}
		
	}
	
	public static class ConnectionEntity {

		private String name;
		private String text;
		private int producercount;
		private int consumercount;
		private int elements;
		private long rowsprocessedcount;
		private String state;
		List<ErrorEntity> messages;

		public ConnectionEntity(ConnectionController connection) {
			ConnectionProperties props = connection.getConnectionProperties();
			this.name = props.getName();
			this.text = props.getPropertyGroup().getText();
			producercount = connection.getProducerCount();
			consumercount = connection.getConsumerCount();
			rowsprocessedcount = connection.getRowsProcessed();
			elements = producercount + consumercount;
			state = connection.getState().name();
			messages = connection.getErrorListRecursive();
		}

		public long getRowsprocessedcount() {
			return rowsprocessedcount;
		}

		public String getName() {
			return name;
		}

		public String getText() {
			return text;
		}

		public int getProducercount() {
			return producercount;
		}

		public int getConsumercount() {
			return consumercount;
		}

		public int getElements() {
			return elements;
		}
		
		public String getState() {
			return state;
		}

		public List<ErrorEntity> getMessages() {
			return messages;
		}

	}
		
}
