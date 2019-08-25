package io.rtdi.bigdata.connector.connectorframework.rest;

import javax.annotation.security.RolesAllowed;
import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

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
