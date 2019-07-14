package io.rtdi.bigdata.fileconnector.service;

import javax.annotation.security.RolesAllowed;
import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.rtdi.bigdata.connector.connectorframework.rest.service.JAXBErrorResponseBuilder;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.fileconnector.entity.CharsetList;

@Path("/")
public class CharsetListService {
	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;

	public CharsetListService() {
	}
			
	@GET
	@Path("/charsets")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed({ServletSecurityConstants.ROLE_VIEW})
    public Response getFileContent() {
		try {
			return Response.ok(new CharsetList()).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

}
	