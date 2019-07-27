package io.rtdi.bigdata.connector.connectorframework.rest.service;

import java.io.IOException;
import java.util.List;

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
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;


@Path("/")
public class HomeService {
	
	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;
		
	@GET
	@Path("/home")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getConnectionProperties() {
		try {
			IPipelineAPI<?, ?, ?, ?> api = WebAppController.getPipelineAPI(servletContext);
			ConnectorController controller = WebAppController.getConnector(servletContext);
			return Response.ok(new HomeEntity(api, controller)).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	public static class HomeEntity {
		private int connectioncount;
		private int topiccount;
		private int schemacount;
		
		public HomeEntity() {
			super();
		}
		
		public HomeEntity(IPipelineAPI<?, ?, ?, ?> api, ConnectorController controller) throws IOException {
			if (controller != null) {
				connectioncount = controller.getConnections().size();
			} else {
				connectioncount = 0;
			}
			if (api != null) {
				List<String> l = api.getTopics();
				if (l != null) {
					topiccount = l.size();
				} else {
					topiccount = 0;
				}
				l = api.getSchemas();
				if (l != null) {
					schemacount = l.size();
				} else {
					schemacount = 0;
				}
			}
		}
		
		public int getConnectioncount() {
			return connectioncount;
		}
		public void setConnectioncount(int connectioncount) {
			this.connectioncount = connectioncount;
		}
		public int getTopiccount() {
			return topiccount;
		}
		public void setTopiccount(int topiccount) {
			this.topiccount = topiccount;
		}
		public int getSchemacount() {
			return schemacount;
		}
		public void setSchemacount(int schemacount) {
			this.schemacount = schemacount;
		}
	}
}
