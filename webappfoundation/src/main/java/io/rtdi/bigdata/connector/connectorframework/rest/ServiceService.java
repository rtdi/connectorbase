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
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.controller.ServiceController;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.MicroServiceTransformation;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerExitType;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ServiceProperties;
import io.rtdi.bigdata.connector.properties.atomic.PropertyRoot;


@Path("/")
public class ServiceService {
	
	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;
		
	@GET
	@Path("/services")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_VIEW)
    public Response getConnections() {
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
    public Response getConnectionProperties(@PathParam("servicename") String servicename) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ServiceController service = connector.getServiceOrFail(servicename);
			return Response.ok(new ServiceConfigEntity(service)).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/service/template")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
    public Response getConnectionPropertiesTemplate() {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			// Create an empty properties structure so the UI can show all properties needed
			return Response.ok(new ServiceConfigEntity(connector.getConnectorFactory().createServiceProperties(null).getPropertyGroup())).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/services/{servicename}/stop")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_OPERATOR)
    public Response stopConnection(@PathParam("servicename") String servicename) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ServiceController service = connector.getServiceOrFail(servicename);
			service.stopController(ControllerExitType.DISABLE);
			boolean stopped = service.joinAll(ControllerExitType.DISABLE);
			return Response.ok(stopped).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/services/{servicename}/start")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_OPERATOR)
    public Response startConnection(@PathParam("servicename") String servicename) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ServiceController service = connector.getServiceOrFail(servicename);
			service.startController(true);
			//TODO: Return if the service was started correctly
			return Response.ok().build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@POST
	@Path("/services/{servicename}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
    public Response setConnectionProperties(@PathParam("servicename") String servicename, ServiceConfigEntity data) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ServiceController service = connector.getService(servicename);
			List<String> services = null;
			if (data.getMicroservices() != null) {
				services = new ArrayList<>();
				for (MicroService s : data.getMicroservices()) {
					services.add(s.getName());
				}
			}
			if (service == null) {
				ServiceProperties<?> props = connector.getConnectorFactory().createServiceProperties(servicename);
				props.setValue(data.getServiceproperties(), services);
				File dir = new File(connector.getConnectorDirectory().getAbsolutePath() + File.separatorChar + "services" + File.separatorChar + servicename);
				if (dir.exists() == false) {
					dir.mkdirs();
				}
				props.write(dir);
				connector.addService(props);
			} else {
				service.getServiceProperties().setValue(data.getServiceproperties(), services);
				service.getServiceProperties().write(service.getDirectory());
			}
			return JAXBSuccessResponseBuilder.getJAXBResponse("created");
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@DELETE
	@Path("/services/{servicename}")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed(ServletSecurityConstants.ROLE_CONFIG)
    public Response deleteConnectionProperties(@PathParam("servicename") String servicename) {
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

		public ServiceEntity(ServiceController service) {
			ServiceProperties<?> props = service.getServiceProperties();
			this.name = props.getName();
			this.text = props.getPropertyGroup().getText();
			rowsprocessedcount = service.getRowsProcessed();
			state = service.getState().name();
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

	}
		
	public static class ServiceConfigEntity {

		private PropertyRoot serviceproperties;
		private List<MicroService> microservices;
		
		public ServiceConfigEntity(ServiceController service) throws PropertiesException {
			serviceproperties = service.getServiceProperties().getPropertyGroupNoPasswords();
			if (service.getMicroservices() != null) {
				microservices = new ArrayList<>();
				for ( MicroServiceTransformation s : service.getMicroservices() ) {
					microservices.add(new MicroService(s.getName()));
				}
			}
		}

		public ServiceConfigEntity() {
			super();
		}

		public ServiceConfigEntity(PropertyRoot props) {
			this();
			serviceproperties = props;
		}

		public PropertyRoot getServiceproperties() {
			return serviceproperties;
		}

		public void setServiceproperties(PropertyRoot serviceproperties) {
			this.serviceproperties = serviceproperties;
		}

		public List<MicroService> getMicroservices() {
			return microservices;
		}

		public void setMicroservices(List<MicroService> microservices) {
			this.microservices = microservices;
		}
		
	}
	
	public static class MicroService {

		private String name;

		public MicroService() {
			super();
		}

		public MicroService(String name) {
			this();
			this.name = name;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}
}
