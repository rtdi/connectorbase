package io.rtdi.bigdata.connector.connectorframework.rest;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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

import io.rtdi.bigdata.connector.connectorframework.IConnectorFactoryService;
import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.controller.ServiceController;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ErrorEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.OperationLogContainer.StateDisplayEntry;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ServiceConfigEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerExitType;
import io.rtdi.bigdata.connector.properties.ServiceProperties;


@Path("/")
public class ServiceService {
	
	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;
	
	private static IConnectorFactoryService getConnectorFactory(ConnectorController connector) {
		return (IConnectorFactoryService) connector.getConnectorFactory();
	}

	@GET
	@Path("/services")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getServices() {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			return Response.ok(new ServicesEntity(connector)).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/services/{servicename}")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getServiceProperties(@PathParam("servicename") String servicename) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ServiceController service = connector.getServiceOrFail(servicename);
			return Response.ok(new ServiceConfigEntity(service.getServiceProperties(), service.getDirectory())).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/services/{servicename}/operationlogs")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getServiceOperationLogs(@PathParam("servicename") String servicename) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ServiceController service = connector.getServiceOrFail(servicename);
			return Response.ok(new ServiceEntity(service)).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/service/template")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
    public Response getServicePropertiesTemplate() {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			// Create an empty properties structure so the UI can show all properties needed
			return Response.ok(new ServiceConfigEntity(getConnectorFactory(connector).createServiceProperties(null).getPropertyGroup())).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/services/{servicename}/stop")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_OPERATOR)
    public Response stopService(@PathParam("servicename") String servicename) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ServiceController service = connector.getServiceOrFail(servicename);
			service.disableController();
			service.joinAll(ControllerExitType.ABORT);
			return JAXBSuccessResponseBuilder.getJAXBResponse("stopped");
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/services/{servicename}/start")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_OPERATOR)
    public Response startService(@PathParam("servicename") String servicename) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ServiceController service = connector.getServiceOrFail(servicename);
			service.startController();
			return JAXBSuccessResponseBuilder.getJAXBResponse("started");
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@POST
	@Path("/services/{servicename}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
    public Response setServiceProperties(@PathParam("servicename") String servicename, ServiceConfigEntity data) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ServiceController service = connector.getService(servicename);
			if (service == null) {
				ServiceProperties props = getConnectorFactory(connector).createServiceProperties(servicename);
				props.setValue(data.getServiceproperties());
				File dir = new File(connector.getConnectorDirectory(), "services" + File.separatorChar + servicename);
				if (dir.exists() == false) {
					dir.mkdirs();
				}
				props.write(dir, data);
				connector.addService(props);
			} else {
				service.getServiceProperties().setValue(data.getServiceproperties());
				service.getServiceProperties().write(service.getDirectory(), data);
			}
			return Response.ok(new ServiceConfigEntity(service.getServiceProperties(), service.getDirectory())).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@DELETE
	@Path("/services/{servicename}")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
    public Response deleteServiceProperties(@PathParam("servicename") String servicename) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ServiceController service = connector.getServiceOrFail(servicename);
			connector.removeService(service);
			return JAXBSuccessResponseBuilder.getJAXBResponse("deleted");
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	public static class ServicesEntity {

		private List<ServiceEntity> services;
		
		public ServicesEntity(ConnectorController connector) {
			if (connector.getConnections() != null) {
				Collection<ServiceController> serviceset = connector.getServices().values();
				this.services = new ArrayList<>();
				for (ServiceController service : serviceset) {
					this.services.add(new ServiceEntity(service));
				}
			}
		}
		
		public List<ServiceEntity> getServices() {
			return services;
		}
		
	}
	
	public static class ServiceEntity {

		private String name;
		private String text;
		private long rowsprocessedcount;
		private String state;
		private List<ErrorEntity> messages;
		private List<StepStatistics> statistics;

		public ServiceEntity(ServiceController service) {
			ServiceProperties props = service.getServiceProperties();
			this.name = props.getName();
			this.text = props.getPropertyGroup().getText();
			rowsprocessedcount = service.getRowsProcessed();
			state = service.getState().name();
			messages = service.getErrorListRecursive();
			Map<String, List<StateDisplayEntry>> operationlogs = service.getMicroserviceOperationLogs();
			if (operationlogs != null) {
				statistics = new ArrayList<>();
				for (String s : operationlogs.keySet()) {
					statistics.add(new StepStatistics(s, operationlogs.get(s)));
				}
			}
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

		public String getState() {
			return state;
		}

		public List<ErrorEntity> getMessages() {
			return messages;
		}

		public List<StepStatistics> getStatistics() {
			return statistics;
		}
	}
	
	public static class StepStatistics {

		private String stepname;
		private List<StateDisplayEntry> operationlogs;

		public StepStatistics(String s, List<StateDisplayEntry> list) {
			this.stepname = s;
			this.operationlogs = list;
		}

		public String getStepname() {
			return stepname;
		}

		public List<StateDisplayEntry> getOperationlogs() {
			return operationlogs;
		}
		
	}
}
