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

import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.rest.entity.landscape.ImpactLineageEntity;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerMetadataEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerMetadataEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ServiceEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ServiceMetadataEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.TopicEntity;

@Path("/")
public class ImpactLineageService {
	
	@Context
    private Configuration configuration;

	@Context
	private ServletContext servletContext;

	@GET
	@Path("/impactlineage")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getImpactLineage() {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			IPipelineAPI<?, ?, ?, ?> api = connector.getPipelineAPI();
			ProducerMetadataEntity producers = api.getProducerMetadata();
			ConsumerMetadataEntity consumers = api.getConsumerMetadata();
			ServiceMetadataEntity services = api.getServiceMetadata();
			ImpactLineageEntity entity = new ImpactLineageEntity();
			if (producers != null) {
				for (ProducerEntity producer : producers.getProducerList()) {
					entity.addProducerNode(producer);
					for (TopicEntity topic : producer.getTopicList()) {
						entity.addProducedTopic(topic.getTopicName(), producer);
					}
				}
			}
			if (consumers != null) {
				for (ConsumerEntity consumer : consumers.getConsumerList()) {
					entity.addConsumerNode(consumer);
					for (String topicname : consumer.getTopicList()) {
						entity.addConsumedTopic(topicname, consumer);
					}
				}
			}
			if (services != null) {
				for (ServiceEntity service : services.getServiceList()) {
					if (service.getProducedTopicList() != null && service.getConsumedTopicList() != null &&
							service.getProducedTopicList().size() != 0 && service.getConsumedTopicList().size() != 0) {
						entity.addServiceNode(service);
						for (TopicEntity topicname : service.getProducedTopicList()) {
							entity.addServiceProducedTopic(topicname.getTopicName(), service);
						}
						for (String topicname : service.getConsumedTopicList()) {
							entity.addServiceConsumedTopic(topicname, service);
						}
					}
				}
			}
			return Response.ok().entity(entity).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

}
