package io.rtdi.bigdata.connector.connectorframework.rest;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
import io.rtdi.bigdata.connector.connectorframework.controller.ProducerController;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorTemporaryException;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerExitType;
import io.rtdi.bigdata.connector.properties.ProducerProperties;
import io.rtdi.bigdata.connector.properties.atomic.PropertyRoot;


@Path("/")
public class ProducerService {
	
	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;
		
	@GET
	@Path("/connections/{connectionname}/producers")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getProducers(@PathParam("connectionname") String connectionname) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnectionOrFail(connectionname);
			return Response.ok(new ProducersEntity(conn)).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/connections/{connectionname}/producers/{producername}")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getProperties(@PathParam("connectionname") String connectionname, @PathParam("producername") String producername) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnectionOrFail(connectionname);
			ProducerController producer = conn.getProducerOrFail(producername);
			return Response.ok(producer.getProducerProperties().getPropertyGroupNoPasswords()).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/connections/{connectionname}/producer/template")
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

	@GET
	@Path("/connections/{connectionname}/producers/{producername}/stop")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_OPERATOR)
    public Response stopProducer(@PathParam("connectionname") String connectionname, @PathParam("producername") String producername) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnectionOrFail(connectionname);
			ProducerController producer = conn.getProducerOrFail(producername);
			producer.controllerDisable();
			boolean stopped = producer.joinAll(ControllerExitType.ABORT);
			return Response.ok(stopped).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/connections/{connectionname}/producers/{producername}/start")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_OPERATOR)
    public Response startProducer(@PathParam("connectionname") String connectionname, @PathParam("producername") String producername) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnectionOrFail(connectionname);
			ProducerController producer = conn.getProducerOrFail(producername);
			if (!conn.isRunning()) {
				throw new ConnectorTemporaryException("Cannot start a producer when its connection is not running", null, "First start the connection", connectionname);
			}
			producer.startController();
			//TODO: Return if the producer was started correctly.
			return Response.ok().build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@POST
	@Path("/connections/{connectionname}/producers/{producername}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
    public Response setProperties(@PathParam("connectionname") String connectionname, @PathParam("producername") String producername, PropertyRoot data) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnectionOrFail(connectionname);
			ProducerController producer = conn.getProducer(producername);
			File dir = new File(conn.getDirectory().getAbsolutePath() + File.separatorChar + ConnectionController.DIR_PRODUCERS);
			if (producer == null) {
				ProducerProperties props = connector.getConnectorFactory().createProducerProperties(producername);
				props.setValue(data);
				if (dir.exists() == false) {
					dir.mkdirs();
				}
				props.write(dir);
				conn.addProducer(props);
			} else {
				producer.getProducerProperties().setValue(data);
				producer.getProducerProperties().write(dir);
			}
			return JAXBSuccessResponseBuilder.getJAXBResponse("created");
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@DELETE
	@Path("/connections/{connectionname}/producers/{producername}")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
    public Response deleteProperties(@PathParam("connectionname") String connectionname, @PathParam("producername") String producername) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnectionOrFail(connectionname);
			ProducerController producer = conn.getProducerOrFail(producername);
			if (conn.removeProducer(producer)) {
				return JAXBSuccessResponseBuilder.getJAXBResponse("deleted");
			} else {
				return JAXBSuccessResponseBuilder.getJAXBResponse("deleted but not shutdown completely");
			}
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}
	
	public static class ProducersEntity {

		private List<ProducerEntity> producers;
		
		public ProducersEntity(ConnectionController connection) {
			if (connection.getProducers() != null) {
				Collection<ProducerController> set = connection.getProducers().values();
				this.producers = new ArrayList<>();
				for (ProducerController producer : set) {
					this.producers.add(new ProducerEntity(producer));
				}
			}
		}
		
		public List<ProducerEntity> getProducers() {
			return producers;
		}
		
	}
	
	public static class ProducerEntity {

		private String name;
		private String text;
		private int instancecount;
		private long rowsprocessedcount;
		private String state;

		public ProducerEntity(ProducerController producer) {
			ProducerProperties props = producer.getProducerProperties();
			this.name = props.getName();
			this.text = props.getPropertyGroup().getText();
			instancecount = producer.getInstanceCount();
			rowsprocessedcount = producer.getRowsProcessedCount();
			state = producer.getState().name();
		}

		public long getRowsprocessedcount() {
			return rowsprocessedcount;
		}

		public String getName() {
			return name;
		}

		public String getText() {
			return text;
		}

		public int getInstancecount() {
			return instancecount;
		}

		public String getState() {
			return state;
		}

	}
		

}
