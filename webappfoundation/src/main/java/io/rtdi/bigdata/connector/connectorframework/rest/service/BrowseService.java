package io.rtdi.bigdata.connector.connectorframework.rest.service;

import java.util.List;

import javax.annotation.security.RolesAllowed;
import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.connectorframework.BrowsingService;
import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.entity.TableEntry;
import io.rtdi.bigdata.connector.connectorframework.rest.entity.SchemaMetadataList;
import io.rtdi.bigdata.connector.connectorframework.rest.entity.SchemaTableData;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;


@Path("/")
public class BrowseService {
	
	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;
		
	@GET
    @Produces(MediaType.APPLICATION_JSON)
	@Path("/connections/{connectionname}/remoteschemas")
	@RolesAllowed({ServletSecurityConstants.ROLE_SCHEMA})
    public Response getRemoteSchemas(@PathParam("connectionname") String connectionname) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnectionOrFail(connectionname);
			try (BrowsingService<?> sourceservice = connector.getConnectorFactory().createBrowsingService(conn);) {
				List<TableEntry> schemas = sourceservice.getRemoteSchemaNames();
				return Response.ok(new SchemaMetadataList(schemas)).build();
			}
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
    @Produces(MediaType.APPLICATION_JSON)
	@Path("/connections/{connectionname}/remoteschemas/{remotename}")
	@RolesAllowed(ServletSecurityConstants.ROLE_SCHEMA)
    public Response getRemoteSchema(@PathParam("connectionname") String connectionname, @PathParam("remotename") String remotename) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnectionOrFail(connectionname);
			try (BrowsingService<?> sourceservice = connector.getConnectorFactory().createBrowsingService(conn);) {
				Schema s = sourceservice.getRemoteSchemaOrFail(remotename);
				return Response.ok(new SchemaTableData(remotename, s)).build();
			}
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

}
