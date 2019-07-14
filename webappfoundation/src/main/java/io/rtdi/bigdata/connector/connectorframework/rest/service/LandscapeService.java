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
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.rest.entity.landscape.LandscapeEntity;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerMetadataEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerMetadataEntity;


@Path("/")
public class LandscapeService {
	
	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;

	@GET
	@Path("/landscape")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getInfrastructure() {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			IPipelineAPI<?, ?, ?, ?> api = connector.getPipelineAPI();
			ProducerMetadataEntity producers = api.getProducerMetadata();
			ConsumerMetadataEntity consumers = api.getConsumerMetadata();
			LandscapeEntity entity = new LandscapeEntity();
			if (producers != null) {
				for (ProducerEntity producer : producers.getProducerList()) {
					entity.addProducerNode(producer);
				}
			}
			if (consumers != null) {
				for (ConsumerEntity consumer : consumers.getConsumerList()) {
					entity.addConsumerNode(consumer);
				}
			}
			return Response.ok().entity(entity).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

}
