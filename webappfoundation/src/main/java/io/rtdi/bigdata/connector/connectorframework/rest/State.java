package io.rtdi.bigdata.connector.connectorframework.rest;

import jakarta.annotation.security.RolesAllowed;
import jakarta.servlet.ServletContext;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Configuration;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;

import io.rtdi.bigdata.connector.connectorframework.rest.entity.GlobalState;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;


@Path("/")
public class State {
	
	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;
	
	@Context
	SecurityContext securityContext;
		
	@GET
    @Produces(MediaType.APPLICATION_JSON)
	@Path("/state")
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getState() {
		try {
			return Response.ok().entity(new GlobalState(servletContext, securityContext)).build(); // error == null is fine, needs to return an empty list of errors
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

}
