package io.rtdi.bigdata.pipelinehttpserver.service;

import java.util.List;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
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
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.JAXBErrorMessage;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.TopicPayload;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.TopicPayloadData;
import io.rtdi.bigdata.pipelinehttpserver.WebAppController;
import io.rtdi.bigdata.pipelinehttpserver.servlet.Index;

@Path("/")
public class DataPreviewService {
	protected final Logger logger = LogManager.getLogger(this.getClass().getName());

	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;
		
	@GET
	@Path("/{tenantid}/data/preview/count/{topicname}/{count}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getData(@PathParam("tenantid") String tenantid, @PathParam("topicname") String topicname, @PathParam("count") int count) {
		try {
			IPipelineServer<?, ?, ?, ?> api = WebAppController.getPipelineAPIOrFail(servletContext);
			List<TopicPayload> data = api.getLastRecords(new TopicName(tenantid, topicname), count);
			TopicPayloadData entity = new TopicPayloadData(data);
			Index.getServerStatisticsHandler().incDataPreview();
			return Response.ok(entity).build();
		} catch (Exception e) {
			logger.info("Calling the Restful service {} ran into an error", servletContext.getContextPath(), e);
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(new JAXBErrorMessage(e)).build();
		}
	}

	@GET
	@Path("/{tenantid}/data/preview/time/{topicname}/{time}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getData(@PathParam("tenantid") String tenantid, @PathParam("topicname") String topicname, @PathParam("time") long time) {
		try {
			IPipelineServer<?, ?, ?, ?> api = WebAppController.getPipelineAPIOrFail(servletContext);
			List<TopicPayload> data = api.getLastRecords(new TopicName(tenantid, topicname), time);
			TopicPayloadData entity = new TopicPayloadData(data);
			Index.getServerStatisticsHandler().incDataPreview();
			return Response.ok(entity).build();
		} catch (Exception e) {
			logger.info("Calling the Restful service {} ran into an error", servletContext.getContextPath(), e);
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(new JAXBErrorMessage(e)).build();
		}
	}

}
