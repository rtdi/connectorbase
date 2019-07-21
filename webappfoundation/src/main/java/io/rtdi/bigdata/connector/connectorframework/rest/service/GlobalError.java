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
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.JAXBErrorMessages;


@Path("/")
public class GlobalError {
	
	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;
		
	@GET
    @Produces(MediaType.APPLICATION_JSON)
	@Path("/error")
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
    public Response getPipelineConnection() {
		try {
			Exception error = WebAppController.getError(servletContext);
			if (error != null) {
				return Response.ok().entity(new JAXBErrorMessages(error)).build();
			} else {
				return Response.ok().build();
			}
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

}
