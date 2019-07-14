package io.rtdi.bigdata.connector.connectorframework.rest.service;

import java.io.File;

import javax.annotation.security.RolesAllowed;
import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
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
	@Path("/connections/{connectionname}")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_OPERATOR)
    public Response getConnectionProperties(@PathParam("connectionname") String connectionname) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnectionOrFail(connectionname);
			return Response.ok(conn.getConnectionProperties().getPropertyGroup()).build();
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
			conn.stopController(ControllerExitType.DISABLE);
			boolean stopped = conn.joinAll(ControllerExitType.DISABLE);
			return Response.ok(stopped).build();
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
			return Response.ok().build();
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
				File dir = new File(connector.getConnectorDirectory().getAbsolutePath() + File.separatorChar + connectionname);
				if (dir.exists() == false) {
					dir.mkdirs();
				}
				props.write(dir);
				connector.addConnection(props);
			} else {
				conn.getConnectionProperties().setValue(data);
				conn.getConnectionProperties().write(conn.getDirectory());
			}
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

}
