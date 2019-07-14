package io.rtdi.bigdata.pipelinehttpserver.service;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineServer;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerMetadataEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.JAXBErrorMessage;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerMetadataEntity;
import io.rtdi.bigdata.pipelinehttpserver.WebAppController;

@Path("/")
public class MetadataService {
	protected final Logger logger = LogManager.getLogger(this.getClass().getName());

	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;
		
	@GET
	@Path("/{tenantid}/meta/producer")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getProducerMetadata(@PathParam("tenantid") String tenantid) {
		try {
			IPipelineServer<?, ?, ?, ?> api = WebAppController.getPipelineAPIOrFail(servletContext);
			ProducerMetadataEntity entity = api.getProducerMetadata(tenantid);
			return Response.ok(entity).build();
		} catch (Exception e) {
			logger.info("Calling the Restful service {} ran into an error", servletContext.getContextPath(), e);
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(new JAXBErrorMessage(e)).build();
		}
	}

	@POST
	@Path("/{tenantid}/meta/producer")
	@Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addProducerMetadata(@PathParam("tenantid") String tenantid, ProducerEntity producer) {
		try {
			IPipelineServer<?, ?, ?, ?> api = WebAppController.getPipelineAPIOrFail(servletContext);
			api.addProducerMetadata(tenantid, producer);
			return Response.ok().build();
		} catch (Exception e) {
			logger.info("Calling the Restful service {} ran into an error", servletContext.getContextPath(), e);
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(new JAXBErrorMessage(e)).build();
		}
	}

	@DELETE
	@Path("/{tenantid}/meta/producer/{producername}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeProducerMetadata(@PathParam("tenantid") String tenantid, @PathParam("producername") String producername) {
		try {
			IPipelineServer<?, ?, ?, ?> api = WebAppController.getPipelineAPIOrFail(servletContext);
			api.removeProducerMetadata(tenantid, producername);
			return Response.ok().build();
		} catch (Exception e) {
			logger.info("Calling the Restful service {} ran into an error", servletContext.getContextPath(), e);
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(new JAXBErrorMessage(e)).build();
		}
	}

	@GET
	@Path("/{tenantid}/meta/consumer")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getConsumerMetadata(@PathParam("tenantid") String tenantid) {
		try {
			IPipelineServer<?, ?, ?, ?> api = WebAppController.getPipelineAPIOrFail(servletContext);
			ConsumerMetadataEntity entity = api.getConsumerMetadata(tenantid);
			return Response.ok(entity).build();
		} catch (Exception e) {
			logger.info("Calling the Restful service {} ran into an error", servletContext.getContextPath(), e);
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(new JAXBErrorMessage(e)).build();
		}
	}

	@POST
	@Path("/{tenantid}/meta/consumer")
	@Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addConsumerMetadata(@PathParam("tenantid") String tenantid, ConsumerEntity consumer) {
		try {
			IPipelineServer<?, ?, ?, ?> api = WebAppController.getPipelineAPIOrFail(servletContext);
			api.addConsumerMetadata(tenantid, consumer);
			return Response.ok().build();
		} catch (Exception e) {
			logger.info("Calling the Restful service {} ran into an error", servletContext.getContextPath(), e);
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(new JAXBErrorMessage(e)).build();
		}
	}

	@DELETE
	@Path("/{tenantid}/meta/consumer/{consumername}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeConsumerMetadata(@PathParam("tenantid") String tenantid, @PathParam("consumername") String consumername) {
		try {
			IPipelineServer<?, ?, ?, ?> api = WebAppController.getPipelineAPIOrFail(servletContext);
			api.removeConsumerMetadata(tenantid, consumername);
			return Response.ok().build();
		} catch (Exception e) {
			logger.info("Calling the Restful service {} ran into an error", servletContext.getContextPath(), e);
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(new JAXBErrorMessage(e)).build();
		}
	}

}
