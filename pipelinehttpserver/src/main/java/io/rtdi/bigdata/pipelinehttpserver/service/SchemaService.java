package io.rtdi.bigdata.pipelinehttpserver.service;

import java.util.ArrayList;
import java.util.List;

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

import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineServer;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaName;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.JAXBErrorMessage;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.SchemaEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.SchemaHandlerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.SchemaListEntity;
import io.rtdi.bigdata.pipelinehttpserver.WebAppController;
import io.rtdi.bigdata.pipelinehttpserver.servlet.Index;

@Path("/")
public class SchemaService {
	protected final Logger logger = LogManager.getLogger(this.getClass().getName());

	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;
		
	@GET
	@Path("/{tenantid}/schema/byname/{schemaname}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSchema(@PathParam("tenantid") String tenantid, @PathParam("schemaname") String schemaname) {
		try {
			IPipelineServer<?, ?, ?, ?> api = WebAppController.getPipelineAPIOrFail(servletContext);
			SchemaHandler h = api.getSchema(new SchemaName(tenantid, schemaname));
			Index.getServerStatisticsHandler().incGetSchema();
			if (h != null) {
				SchemaHandlerEntity entity = new SchemaHandlerEntity(h);
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
	@Path("/{tenantid}/schema/byid/{schemaid}")
	@Produces(MediaType.APPLICATION_JSON)
    public Response getSchema(@PathParam("tenantid") String tenantid, @PathParam("schemaid") int schemaid) {
		try {
			IPipelineServer<?, ?, ?, ?> api = WebAppController.getPipelineAPIOrFail(servletContext);
			Schema s = api.getSchema(schemaid);
			Index.getServerStatisticsHandler().incGetSchema();
			if (s != null) {
				SchemaEntity entity = new SchemaEntity(s);
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
	@Path("/{tenantid}/schema/byname/{schemaname}")
	@Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response registerSchema(@PathParam("tenantid") String tenantid, @PathParam("schemaname") String schemaname, SchemaHandlerEntity entityin) {
		try {
			IPipelineServer<?, ?, ?, ?> api = WebAppController.getPipelineAPIOrFail(servletContext);
			SchemaHandler h = api.registerSchema(new SchemaName(tenantid, schemaname), entityin.getDescription(), entityin.getKeySchema(), entityin.getValueSchema());
			SchemaHandlerEntity entity = new SchemaHandlerEntity(h);
			Index.getServerStatisticsHandler().incRegisterSchema();
			return Response.ok(entity).build();
		} catch (Exception e) {
			logger.info("Calling the Restful service {} ran into an error", servletContext.getContextPath(), e);
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(new JAXBErrorMessage(e)).build();
		}
	}

	@GET
	@Path("/{tenantid}/schema/list")
	@Produces(MediaType.APPLICATION_JSON)
    public Response getSchema(@PathParam("tenantid") String tenantid) {
		try {
			IPipelineServer<?, ?, ?, ?> api = WebAppController.getPipelineAPIOrFail(servletContext);
			List<String> schemalist = api.getSchemas(tenantid);
			if (schemalist == null) {
				schemalist = new ArrayList<>(); // should not happen but hey
			}
			SchemaListEntity entity = new SchemaListEntity(schemalist);
			Index.getServerStatisticsHandler().incGetSchemas();
			return Response.ok(entity).build();
		} catch (Exception e) {
			logger.info("Calling the Restful service {} ran into an error", servletContext.getContextPath(), e);
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(new JAXBErrorMessage(e)).build();
		}
	}

}
