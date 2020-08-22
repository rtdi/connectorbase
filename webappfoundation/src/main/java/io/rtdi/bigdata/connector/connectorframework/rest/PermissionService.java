package io.rtdi.bigdata.connector.connectorframework.rest;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import jakarta.annotation.security.RolesAllowed;
import javax.servlet.ServletContext;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Configuration;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;

import io.rtdi.bigdata.connector.connectorframework.RolesAllowedDynamicFeature2;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;


@Path("/")
public class PermissionService {
	
	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;
	
	@Context 
	private SecurityContext securityContext;
		
	@GET
    @Produces(MediaType.APPLICATION_JSON)
	@Path("/permissions")
	@RolesAllowed({ServletSecurityConstants.ROLE_VIEW})
    public Response getPermissions() {
		try {
			Map<String, Map<String, Map<String, Set<String>>>> permissions = RolesAllowedDynamicFeature2.getRoles();
			Map<String, Map<String, Set<String>>> userpermissions = new HashMap<>();
			for (String role : permissions.keySet()) {
				if (securityContext.isUserInRole(role)) {
					userpermissions.putAll(permissions.get(role));
				}
			}
			return Response.ok(userpermissions).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

}
