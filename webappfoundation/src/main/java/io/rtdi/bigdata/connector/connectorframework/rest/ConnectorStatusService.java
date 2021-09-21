package io.rtdi.bigdata.connector.connectorframework.rest;

import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.entity.UsageStatistics;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerExitType;
import jakarta.annotation.security.RolesAllowed;
import jakarta.servlet.ServletContext;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Configuration;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;


@Path("/")
public class ConnectorStatusService {
	
	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;
		
	@GET
    @Produces(MediaType.APPLICATION_JSON)
	@Path("/ConnectorStatus")
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
	public Response getConnectorStatus() {
		try {
			ConnectorController controller = WebAppController.getConnectorOrFail(servletContext);
			return Response.ok(new UsageStatistics(controller, null)).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
    @Produces(MediaType.APPLICATION_JSON)
	@Path("/ConnectorStatus/start")
	@RolesAllowed(ServletSecurityConstants.ROLE_OPERATOR)
	public Response start() {
		try {
			ConnectorController controller = WebAppController.getConnectorOrFail(servletContext);
			controller.stopController(ControllerExitType.ABORT);
			controller.getPipelineAPI().reloadConnectionProperties();
			controller.readConfigs();
			controller.startController();
			return Response.ok().build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
    @Produces(MediaType.APPLICATION_JSON)
	@Path("/ConnectorStatus/stop")
	@RolesAllowed(ServletSecurityConstants.ROLE_OPERATOR)
	public Response stop() {
		try {
			ConnectorController controller = WebAppController.getConnectorOrFail(servletContext);
			controller.stopController(ControllerExitType.ABORT);
			return Response.ok().build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}
}