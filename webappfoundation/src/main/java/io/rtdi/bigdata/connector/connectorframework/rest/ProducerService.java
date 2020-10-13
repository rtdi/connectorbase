package io.rtdi.bigdata.connector.connectorframework.rest;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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

import io.rtdi.bigdata.connector.connectorframework.IConnectorFactoryProducer;
import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.controller.ProducerController;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorTemporaryException;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.PipelineAbstract;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ErrorEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.LoadInfo;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerExitType;
import io.rtdi.bigdata.connector.properties.ProducerProperties;
import io.rtdi.bigdata.connector.properties.atomic.PropertyRoot;


@Path("/")
public class ProducerService {
	
	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;
	
	private static IConnectorFactoryProducer<?,?> getConnectorFactory(ConnectorController connector) {
		return (IConnectorFactoryProducer<?, ?>) connector.getConnectorFactory();
	}

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
	@Path("/connections/{connectionname}/producers/{producername}/transactions")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getTransactions(@PathParam("connectionname") String connectionname, @PathParam("producername") String producername) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			Map<Integer, Map<String, LoadInfo>> m = connector.getPipelineAPI().getLoadInfo(producername);
			TransactionInfo t = new TransactionInfo(m);
			return Response.ok(t).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}
	
	@POST
	@Path("/connections/{connectionname}/producers/{producername}/transactions")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
    public Response setTransactions(@PathParam("connectionname") String connectionname, @PathParam("producername") String producername, TransactionInfo data) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnectionOrFail(connectionname);
			ProducerController producer = conn.getProducer(producername);
			producer.disableController(); // stop controller and keep it stopped so it is not recovered automatically
			producer.joinAll(ControllerExitType.ABORT);
			if (data.getProducertransactions() != null) {
				for (ProducerTransactions p : data.getProducertransactions()) {
					if (p.getResetdelta() != null && p.getResetdelta()) {
						connector.getPipelineAPI().rewindDeltaLoad(producername, p.getInstanceno(), p.getDeltatransaction().getTransactionid());
					}
					if (p.getInitialloadtransactions() != null) {
						for (SchemaTransaction init : p.getInitialloadtransactions()) {
							if (init.getReset() != null && init.getReset()) {
								connector.getPipelineAPI().resetInitialLoad(producername, init.getSchemaname(), p.getInstanceno());
							}
						}
					}
				}
			}
			producer.startController();
			return JAXBSuccessResponseBuilder.getJAXBResponse("created");
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
			return Response.ok(getConnectorFactory(connector).createProducerProperties(null).getPropertyGroup()).build();
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
			producer.disableController();
			producer.joinAll(ControllerExitType.ABORT);
			return JAXBSuccessResponseBuilder.getJAXBResponse("stopped");
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
			File dir = new File(conn.getDirectory(), ConnectionController.DIR_PRODUCERS);
			producer.getProducerProperties().read(dir);
			producer.startController();
			//TODO: Return if the producer was started correctly.
			return JAXBSuccessResponseBuilder.getJAXBResponse("started");
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
			File dir = new File(conn.getDirectory(), ConnectionController.DIR_PRODUCERS);
			if (producer == null) {
				ProducerProperties props = getConnectorFactory(connector).createProducerProperties(producername);
				props.setValue(data);
				if (dir.exists() == false) {
					dir.mkdirs();
				}
				props.write(dir);
				producer = conn.addProducer(props);
			} else {
				producer.stopController(ControllerExitType.ABORT);
				producer.getProducerProperties().setValue(data);
				producer.getProducerProperties().write(dir);
			}
			producer.startController();
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
		List<ErrorEntity> messages;
		private List<String> instancestates;

		public ProducerEntity(ProducerController producer) {
			ProducerProperties props = producer.getProducerProperties();
			this.name = props.getName();
			this.text = props.getPropertyGroup().getText();
			instancecount = producer.getInstanceCount();
			rowsprocessedcount = producer.getRowsProcessedCount();
			state = producer.getState().name();
			messages = producer.getErrorListRecursive();
			instancestates = producer.getInstanceStates();
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

		public List<String> getInstancestates() {
			return instancestates;
		}

	}
	
	public static class TransactionInfo {
		List<ProducerTransactions> p;
		
		public TransactionInfo() {
		}
		
		public TransactionInfo(Map<Integer, Map<String, LoadInfo>> m) {
			if (m != null) {
				p = new ArrayList<>();
				for (Integer instanceno : m.keySet()) {
					p.add(new ProducerTransactions(instanceno, m.get(instanceno)));
				}
			}
		}

		public List<ProducerTransactions> getProducertransactions() {
			return p;
		}

		public void setProducertransactions(List<ProducerTransactions> p) {
			this.p = p;
		}
		
	}
	
	public static class ProducerTransactions {

		private Integer instanceno;
		private List<SchemaTransaction> schematransactions;
		private LoadInfo deltatransaction;
		private Boolean resetdelta; 

		public ProducerTransactions() {
		}
		
		public ProducerTransactions(Integer instanceno, Map<String, LoadInfo> schemamap) {
			this.instanceno = instanceno;
			if (schemamap != null) {
				schematransactions = new ArrayList<>();
				for (String s : schemamap.keySet()) {
					if (s.equals(PipelineAbstract.ALL_SCHEMAS)) {
						deltatransaction = schemamap.get(s);
					} else {
						schematransactions.add(new SchemaTransaction(s, schemamap.get(s)));
					}
				}
			}
		}

		public Integer getInstanceno() {
			return instanceno;
		}

		public List<SchemaTransaction> getInitialloadtransactions() {
			return schematransactions;
		}

		public LoadInfo getDeltatransaction() {
			return deltatransaction;
		}

		public void setInstanceno(Integer instanceno) {
			this.instanceno = instanceno;
		}

		public void setInitialloadtransactions(List<SchemaTransaction> schematransactions) {
			this.schematransactions = schematransactions;
		}

		public void setDeltatransaction(LoadInfo deltatransaction) {
			this.deltatransaction = deltatransaction;
		}

		public Boolean getResetdelta() {
			return resetdelta;
		}

		public void setResetdelta(Boolean resetdelta) {
			this.resetdelta = resetdelta;
		}
		
	}
	
	public static class SchemaTransaction {

		private String schemaname;
		private LoadInfo transaction;
		private Boolean reset; 

		public SchemaTransaction() {
		}

		public SchemaTransaction(String schemaname, LoadInfo transaction) {
			this.schemaname = schemaname;
			this.transaction = transaction;
		}

		public String getSchemaname() {
			return schemaname;
		}

		public LoadInfo getTransaction() {
			return transaction;
		}

		public Boolean getReset() {
			return reset;
		}

		public void setReset(Boolean reset) {
			this.reset = reset;
		}

		public void setSchemaname(String schemaname) {
			this.schemaname = schemaname;
		}

		public void setTransaction(LoadInfo transaction) {
			this.transaction = transaction;
		}
		
	}

}
