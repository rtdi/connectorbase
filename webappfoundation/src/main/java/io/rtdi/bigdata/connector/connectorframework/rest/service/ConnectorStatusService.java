package io.rtdi.bigdata.connector.connectorframework.rest.service;

import javax.annotation.security.RolesAllowed;
import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.entity.UsageStatistics;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;


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
}