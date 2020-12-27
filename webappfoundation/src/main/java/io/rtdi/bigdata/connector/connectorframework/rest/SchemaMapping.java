package io.rtdi.bigdata.connector.connectorframework.rest;

import jakarta.annotation.security.RolesAllowed;
import jakarta.servlet.ServletContext;
import jakarta.ws.rs.Consumes;
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
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.entity.SchemaMappingData;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaRegistryName;


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
			SchemaHandler targetschemhandler = connector.getPipelineAPI().getSchema(SchemaRegistryName.create(targetschemaname)); 
			data.save(connector, connectionname, remoteschemaname, targetschemhandler);
			return JAXBSuccessResponseBuilder.getJAXBResponse("saved");
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

}
