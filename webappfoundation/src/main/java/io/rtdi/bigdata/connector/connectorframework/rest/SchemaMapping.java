package io.rtdi.bigdata.connector.connectorframework.rest;

import javax.annotation.security.RolesAllowed;
import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
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
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.entity.SchemaMappingData;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;


@Path("/")
public class SchemaMapping {
	
	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;
		
	@GET
	@Path("/mapping/{connectionname}/{remoteschemaname}/{targetschemaname}")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getMapping(
    		@PathParam("connectionname") String connectionname,
    		@PathParam("remoteschemaname") String remoteschemaname,
    		@PathParam("targetschemaname") String targetschemaname) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			return Response.ok(new SchemaMappingData(connector, connectionname, remoteschemaname, targetschemaname)).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@POST
	@Path("/mapping/{connectionname}/{remoteschemaname}/{targetschemaname}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
    public Response setMapping(
    		@PathParam("connectionname") String connectionname,
    		@PathParam("remoteschemaname") String remoteschemaname,
    		@PathParam("targetschemaname") String targetschemaname,
    		SchemaMappingData data) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			SchemaHandler targetschemhandler = connector.getPipelineAPI().getSchema(targetschemaname); 
			data.save(connector, connectionname, remoteschemaname, targetschemhandler);
			return JAXBSuccessResponseBuilder.getJAXBResponse("saved");
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

}
