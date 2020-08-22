package io.rtdi.bigdata.connector.connectorframework.rest;

import jakarta.annotation.security.RolesAllowed;
import javax.servlet.ServletContext;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Configuration;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
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
			ConnectorController connectorcontroller = WebAppController.getConnector(servletContext);
			IPipelineAPI<?, ?, ?, ?> api = connectorcontroller.getPipelineAPI();
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
			WebAppController.clearError(servletContext);
			ConnectorController connectorcontroller = WebAppController.getConnector(servletContext);
			IPipelineAPI<?, ?, ?, ?> api = connectorcontroller.getPipelineAPI();
			api.getAPIProperties().setValue(data);
			api.writeConnectionProperties();
			api.reloadConnectionProperties();
			api.validate();
			connectorcontroller.readConfigs();
			connectorcontroller.startController();
			return JAXBSuccessResponseBuilder.getJAXBResponse("created");
		} catch (Exception e) {
			WebAppController.setError(servletContext, e);
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}
}
