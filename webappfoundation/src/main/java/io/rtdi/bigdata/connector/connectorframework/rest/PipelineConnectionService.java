package io.rtdi.bigdata.connector.connectorframework.rest;

import javax.annotation.security.RolesAllowed;
import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerExitType;
import io.rtdi.bigdata.connector.properties.atomic.PropertyRoot;


@Path("/")
public class PipelineConnectionService {
	
	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;
		
	@GET
    @Produces(MediaType.APPLICATION_JSON)
	@Path("/PipelineConnection")
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getPipelineConnection() {
		try {
			IPipelineAPI<?, ?, ?, ?> api = WebAppController.getPipelineAPI(servletContext);
			return Response.ok(api.getAPIProperties().getPropertyGroupNoPasswords()).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@POST
    @Produces(MediaType.APPLICATION_JSON)
	@Path("/PipelineConnection")
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
    public Response postPipelineConnection(PropertyRoot data) {
		try {
			IPipelineAPI<?, ?, ?, ?> api = WebAppController.getPipelineAPI(servletContext);
			api.getAPIProperties().setValue(data);
			api.writeConnectionProperties();
			api.reloadConnectionProperties();
			WebAppController.clearError(servletContext);
			ConnectorController connectorcontroller = WebAppController.getConnector(servletContext);
			connectorcontroller.stopController(ControllerExitType.ABORT);
			connectorcontroller.joinAll(ControllerExitType.ABORT);
			connectorcontroller.readConfigs();
			connectorcontroller.startController(false);
			return Response.ok("created").build();
		} catch (Exception e) {
			WebAppController.setError(servletContext, e);
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}
}
