package io.rtdi.bigdata.connector.connectorframework.rest;

import java.io.IOException;
import java.util.List;

import javax.annotation.security.RolesAllowed;
import javax.servlet.ServletContext;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Configuration;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import io.rtdi.bigdata.connector.connectorframework.IConnectorFactory;
import io.rtdi.bigdata.connector.connectorframework.IConnectorFactoryService;
import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.entity.PipelineName;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;


@Path("/")
public class HomeService {
	private static List<PipelineName> pipelineAPIsAvailable = null;

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
			ConnectorController controller = WebAppController.getConnector(servletContext);
			IConnectorFactory<?> factory = WebAppController.getConnectorFactory(servletContext);
			return Response.ok(new HomeEntity(controller, factory)).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	public static class HomeEntity {
		private int connectioncount;
		private int servicecount;
		private int topiccount;
		private int schemacount;
		private int producercount;
		private int consumercount;
		private long rowsprocessedcount;
		private Long lastprocessed = null;
		private boolean supportsconnections = true;
		private boolean supportsservices = true;
		private String pipelineAPIName;
		private boolean pipelineAPIConfigured;
		private String connectorhelpurl;
		
		public HomeEntity() {
			super();
		}
		
		public HomeEntity(ConnectorController controller, IConnectorFactory<?> factory) throws IOException {
			if (factory != null) {
				supportsconnections = factory instanceof IConnectorFactory<?>;
				supportsservices = factory instanceof IConnectorFactoryService;
			}
			if (controller != null) {
				if (pipelineAPIsAvailable == null) {
					pipelineAPIsAvailable = controller.getPipelineAPIsAvailable();
				}
				connectorhelpurl = controller.getConnectorHelpURL();
				IPipelineAPI<?, ?, ?, ?> api = controller.getPipelineAPI();
				if (api != null) {
					pipelineAPIName = api.getClass().getSimpleName();
					pipelineAPIConfigured = api.getAPIProperties() != null;
					if (pipelineAPIConfigured) {
						if (!controller.isRunning()) {
							api.reloadConnectionProperties();
							controller.startController();
						}
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
						connectioncount = controller.getConnections().size();
						producercount = controller.getProducerCount();
						consumercount = controller.getConsumerCount();
						rowsprocessedcount = controller.getRowsProcessed();
						lastprocessed = controller.getLastProcessed();
						servicecount = controller.getServices().size();
					} else {
					}
				}
			} else {
				connectioncount = 0;
				servicecount = 0;
			}
		}
		
		public int getConnectioncount() {
			return connectioncount;
		}
		public int getServicecount() {
			return servicecount;
		}
		public int getTopiccount() {
			return topiccount;
		}
		public int getSchemacount() {
			return schemacount;
		}
		public int getProducercount() {
			return producercount;
		}

		public int getConsumercount() {
			return consumercount;
		}

		public long getRowsprocessedcount() {
			return rowsprocessedcount;
		}
		public Long getLastprocessed() {
			return lastprocessed;
		}
		
		public boolean isSupportconnections() {
			return supportsconnections;
		}
		public boolean isSupportservices() {
			return supportsservices;
		}

		public List<PipelineName> getPipelineAPIsAvailable() {
			return pipelineAPIsAvailable;
		}

		public String getPipelineAPIName() {
			return pipelineAPIName;
		}

		public boolean isPipelineAPIConfigured() {
			return pipelineAPIConfigured;
		}

		public String getConnectorhelpurl() {
			return connectorhelpurl;
		}
	}
}
