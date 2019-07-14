package io.rtdi.bigdata.connector.connectorframework.rest.service;

import java.io.File;

import javax.annotation.security.RolesAllowed;
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

import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConsumerController;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;
import io.rtdi.bigdata.connector.properties.atomic.PropertyRoot;



@Path("/")
public class ConsumerService {
	
	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;
		
	@GET
	@Path("/connections/{connectionname}/consumers/{consumername}")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
    public Response getProperties(@PathParam("connectionname") String connectionname, @PathParam("consumername") String consumername) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnectionOrFail(connectionname);
			ConsumerController producer = conn.getConsumerOrFail(consumername);
			return Response.ok(producer.getConsumerProperties().getPropertyGroup()).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/connections/{connectionname}/consumer/template")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
    public Response getPropertiesTemplate(@PathParam("connectionname") String connectionname) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			// Create an empty properties structure so the UI can show all properties needed
			return Response.ok(connector.getConnectorFactory().createProducerProperties(null).getPropertyGroup()).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@POST
	@Path("/connections/{connectionname}/consumers/{consumername}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
    public Response setProperties(@PathParam("connectionname") String connectionname, @PathParam("consumername") String consumername, PropertyRoot data) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnectionOrFail(connectionname);
			ConsumerController consumer = conn.getConsumer(consumername);
			File dir = new File(conn.getDirectory().getAbsolutePath() + File.separatorChar + ConnectionController.DIR_PRODUCERS);
			if (consumer == null) {
				ConsumerProperties props = connector.getConnectorFactory().createConsumerProperties(consumername);
				props.setValue(data);
				if (dir.exists() == false) {
					dir.mkdirs();
				}
				props.write(dir);
				conn.addConsumer(props);
			} else {
				consumer.getConsumerProperties().setValue(data);
				consumer.getConsumerProperties().write(dir);
			}
			return JAXBSuccessResponseBuilder.getJAXBResponse("created");
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@DELETE
	@Path("/connections/{connectionname}/consumers/{consumername}")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
    public Response deleteProperties(@PathParam("connectionname") String connectionname, @PathParam("consumername") String consumername) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnectionOrFail(connectionname);
			ConsumerController consumer = conn.getConsumerOrFail(consumername);
			if (conn.removeConsumer(consumer)) {
				return JAXBSuccessResponseBuilder.getJAXBResponse("deleted");
			} else {
				return JAXBSuccessResponseBuilder.getJAXBResponse("deleted but not shutdown completely");
			}
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

}
