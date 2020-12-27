package io.rtdi.bigdata.connector.connectorframework.rest;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import jakarta.annotation.security.RolesAllowed;
import jakarta.servlet.ServletContext;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Configuration;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import io.rtdi.bigdata.connector.connectorframework.IConnectorFactoryConsumer;
import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConsumerController;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorTemporaryException;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.Epoch;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ErrorEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerExitType;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;
import io.rtdi.bigdata.connector.properties.atomic.PropertyRoot;



@Path("/")
public class ConsumerService {
	
	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;
	
	private static IConnectorFactoryConsumer<?,?> getConnectorFactory(ConnectorController connector) {
		return (IConnectorFactoryConsumer<?, ?>) connector.getConnectorFactory();
	}
	
	@GET
	@Path("/connections/{connectionname}/consumers")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getConsumers(@PathParam("connectionname") String connectionname) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnectionOrFail(connectionname);
			return Response.ok(new ConsumersEntity(conn)).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/connections/{connectionname}/consumers/{consumername}")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getProperties(@PathParam("connectionname") String connectionname, @PathParam("consumername") String consumername) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnectionOrFail(connectionname);
			ConsumerController consumer = conn.getConsumerOrFail(consumername);
			return Response.ok(consumer.getConsumerProperties().getPropertyGroupNoPasswords()).build();
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
			return Response.ok(getConnectorFactory(connector).createConsumerProperties(null).getPropertyGroup()).build();
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
			File dir = new File(conn.getDirectory(), ConnectionController.DIR_CONSUMERS);
			if (consumer == null) {
				ConsumerProperties props = getConnectorFactory(connector).createConsumerProperties(consumername);
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

	@GET
	@Path("/connections/{connectionname}/consumers/{consumername}/stop")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_OPERATOR)
    public Response stopConsumer(@PathParam("connectionname") String connectionname, @PathParam("consumername") String consumername) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnectionOrFail(connectionname);
			ConsumerController consumer = conn.getConsumerOrFail(consumername);
			consumer.disableController();
			consumer.joinAll(ControllerExitType.ABORT);
			return JAXBSuccessResponseBuilder.getJAXBResponse("stopped");
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/connections/{connectionname}/consumers/{consumername}/state")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getConsumerState(@PathParam("connectionname") String connectionname, @PathParam("consumername") String consumername) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnectionOrFail(connectionname);
			ConsumerController consumer = conn.getConsumerOrFail(consumername);
			Long l = consumer.getLastOffsetTimestamp();
			if (l == null) {
				l = System.currentTimeMillis();
			}
			Epoch epoch = new Epoch(l);
			return Response.ok(epoch).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@POST
	@Path("/connections/{connectionname}/consumers/{consumername}/state")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
    public Response setConsumerOffsets(@PathParam("connectionname") String connectionname, @PathParam("consumername") String consumername, Epoch data) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnectionOrFail(connectionname);
			ConsumerController consumer = conn.getConsumerOrFail(consumername);
			consumer.disableController();
			consumer.joinAll(ControllerExitType.ABORT);
			connector.getPipelineAPI().rewindConsumer(consumer.getConsumerProperties(), data.getEpoch());
			consumer.startController();
			return JAXBSuccessResponseBuilder.getJAXBResponse("altered");
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/connections/{connectionname}/consumers/{consumername}/start")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_OPERATOR)
    public Response startConsumer(@PathParam("connectionname") String connectionname, @PathParam("consumername") String consumername) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnectionOrFail(connectionname);
			ConsumerController consumer = conn.getConsumerOrFail(consumername);
			if (!conn.isRunning()) {
				throw new ConnectorTemporaryException("Cannot start a consumer when its connection is not running", null, "First start the connection", connectionname);
			}
			File dir = new File(conn.getDirectory(), ConnectionController.DIR_CONSUMERS);
			consumer.getConsumerProperties().read(dir);
			consumer.startController();
			//TODO: Return if the producer was started correctly.
			return JAXBSuccessResponseBuilder.getJAXBResponse("started");
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

	public static class ConsumersEntity {

		private List<ConsumerEntity> consumers;
		
		public ConsumersEntity(ConnectionController connection) {
			if (connection.getProducers() != null) {
				Collection<ConsumerController> set = connection.getConsumers().values();
				this.consumers = new ArrayList<>();
				for (ConsumerController consumer : set) {
					this.consumers.add(new ConsumerEntity(consumer));
				}
			}
		}
		
		public List<ConsumerEntity> getConsumers() {
			return consumers;
		}
		
	}
	
	public static class ConsumerEntity {

		private String name;
		private String text;
		private int instancecount;
		private long rowsprocessedcount;
		private String state;
		List<ErrorEntity> messages;
		List<String> instanceoperations;

		public ConsumerEntity(ConsumerController consumer) {
			ConsumerProperties props = consumer.getConsumerProperties();
			this.name = props.getName();
			this.text = props.getPropertyGroup().getText();
			instancecount = consumer.getInstanceCount();
			rowsprocessedcount = consumer.getRowsProcessedCount();
			state = consumer.getState().name();
			messages = consumer.getErrorListRecursive();
			instanceoperations = consumer.getInstanceOperations();
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

		public List<ErrorEntity> getMessages() {
			return messages;
		}
		
		public List<String> getInstanceOperations() {
			return instanceoperations;
		}

	}
	
}
