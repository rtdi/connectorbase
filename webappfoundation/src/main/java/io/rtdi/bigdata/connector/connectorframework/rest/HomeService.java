package io.rtdi.bigdata.connector.connectorframework.rest;

import java.io.IOException;
import java.util.List;

import io.rtdi.bigdata.connector.connectorframework.IConnectorFactory;
import io.rtdi.bigdata.connector.connectorframework.IConnectorFactoryService;
import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.entity.PipelineName;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaRegistryName;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import jakarta.annotation.security.RolesAllowed;
import jakarta.servlet.ServletContext;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Configuration;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;


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
			return Response.ok(new HomeEntity(controller, factory, servletContext)).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	public static class HomeEntity {
		private int connectioncount = 0;
		private int servicecount = 0;
		private int topiccount = 0;
		private int schemacount = 0;
		private int producercount = 0;
		private int consumercount = 0;
		private long rowsprocessedcount = 0L;
		private Long lastprocessed = null;
		private boolean supportsconnections = true;
		private boolean supportsservices = true;
		private String pipelineAPIName;
		private boolean pipelineAPIConfigured;
		private String connectorhelpurl;
		
		public HomeEntity() {
			super();
		}
		
		public HomeEntity(ConnectorController controller, IConnectorFactory<?> factory, ServletContext servletContext) throws IOException {
			if (factory != null) {
				supportsconnections = !(factory instanceof IConnectorFactoryService);
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
						connectioncount = controller.getConnections().size();
						producercount = controller.getProducerCount();
						consumercount = controller.getConsumerCount();
						rowsprocessedcount = controller.getRowsProcessed();
						lastprocessed = controller.getLastProcessed();
						servicecount = controller.getServices().size();
						if (controller.isRunning()) {
							/*
							 * If the controller is active
							 */
							List<TopicName> l = api.getTopics();
							if (l != null) {
								topiccount = l.size();
							}
							List<SchemaRegistryName> k = api.getSchemas();
							if (k != null) {
								schemacount = k.size();
							}
						}
					}
				}
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
