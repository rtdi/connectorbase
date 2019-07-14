package io.rtdi.bigdata.pipelinehttpserver.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
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
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.JAXBErrorMessage;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.TopicHandlerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.TopicListEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.TopicHandlerEntity.ConfigPair;
import io.rtdi.bigdata.pipelinehttpserver.WebAppController;
import io.rtdi.bigdata.pipelinehttpserver.servlet.Index;

@Path("/")
public class TopicService {
	protected final Logger logger = LogManager.getLogger(this.getClass().getName());

	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;
		
	@GET
	@Path("/{tenantid}/topic/byname/{topicname}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTopic(@PathParam("tenantid") String tenantid, @PathParam("topicname") String topicname) {
		try {
			IPipelineServer<?, ?, ?, ?> api = WebAppController.getPipelineAPIOrFail(servletContext);
			TopicHandler h = api.getTopic(new TopicName(tenantid, topicname));
			if (h != null) {
				TopicHandlerEntity entity = new TopicHandlerEntity(h);
				Index.getServerStatisticsHandler().incGetTopic();
				return Response.ok(entity).build();
			} else {
				return Response.status(Status.NO_CONTENT).build();
			}
		} catch (Exception e) {
			logger.info("Calling the Restful service {} ran into an error", servletContext.getContextPath(), e);
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(new JAXBErrorMessage(e)).build();
		}
	}

	@POST
	@Path("/{tenantid}/topic/byname/{topicname}")
	@Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response newTopic(@PathParam("tenantid") String tenantid, @PathParam("topicname") String topicname, TopicHandlerEntity entityin) {
		try {
			IPipelineServer<?, ?, ?, ?> api = WebAppController.getPipelineAPIOrFail(servletContext);
			Map<String, String> configs = null;
			List<ConfigPair> topiclist = entityin.getConfiglist();
			if (topiclist != null) {
				configs = new HashMap<>();
				for (ConfigPair configpair : topiclist) {
					configs.put(configpair.getKey(), configpair.getValue());
				}
			}
			TopicHandler h = api.createTopic(new TopicName(tenantid, topicname), entityin.getPartitioncount(), entityin.getReplicationfactor(), configs);
			Index.getServerStatisticsHandler().incNewTopic();
			if (h != null) {
				TopicHandlerEntity entity = new TopicHandlerEntity(h);
				return Response.ok(entity).build();
			} else {
				return Response.status(Status.NO_CONTENT).build();
			}
		} catch (Exception e) {
			logger.info("Calling the Restful service {} ran into an error", servletContext.getContextPath(), e);
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(new JAXBErrorMessage(e)).build();
		}
	}

	@GET
	@Path("/{tenantid}/topic/list")
	@Produces(MediaType.APPLICATION_JSON)
    public Response getTopics(@PathParam("tenantid") String tenantid) {
		try {
			IPipelineServer<?, ?, ?, ?> api = WebAppController.getPipelineAPIOrFail(servletContext);
			List<String> topiclist = api.getTopics(tenantid);
			TopicListEntity entity = new TopicListEntity(topiclist);
			Index.getServerStatisticsHandler().incGetTopics();
			return Response.ok(entity).build();
		} catch (Exception e) {
			logger.info("Calling the Restful service {} ran into an error", servletContext.getContextPath(), e);
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(new JAXBErrorMessage(e)).build();
		}
	}

}
