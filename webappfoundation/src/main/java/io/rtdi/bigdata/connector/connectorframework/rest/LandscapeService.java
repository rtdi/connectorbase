package io.rtdi.bigdata.connector.connectorframework.rest;

import javax.annotation.security.RolesAllowed;
import javax.servlet.ServletContext;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Configuration;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.rest.entity.landscape.LandscapeEntity;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerMetadataEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerMetadataEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ServiceEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ServiceMetadataEntity;


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
			ServiceMetadataEntity services = api.getServiceMetadata();
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
			if (services != null) {
				for (ServiceEntity service : services.getServiceList()) {
					entity.addServiceNode(service);
				}
			}
			return Response.ok().entity(entity).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

}
