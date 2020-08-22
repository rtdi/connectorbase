package io.rtdi.bigdata.connector.connectorframework.rest;

import java.util.List;

import jakarta.annotation.security.RolesAllowed;
import javax.servlet.ServletContext;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Configuration;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

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
			BrowsingService<?> sourceservice = conn.getBrowser();
			List<TableEntry> schemas = sourceservice.getRemoteSchemaNames();
			return Response.ok(new SchemaMetadataList(schemas)).build();
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
			BrowsingService<?> sourceservice = conn.getBrowser();
			Schema s = sourceservice.getRemoteSchemaOrFail(remotename);
			return Response.ok(new SchemaTableData(remotename, s)).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@DELETE
    @Produces(MediaType.APPLICATION_JSON)
	@Path("/connections/{connectionname}/remoteschemas/{remotename}")
	@RolesAllowed(ServletSecurityConstants.ROLE_SCHEMA)
    public Response deleteRemoteSchema(@PathParam("connectionname") String connectionname, @PathParam("remotename") String remotename) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController conn = connector.getConnectionOrFail(connectionname);
			BrowsingService<?> sourceservice = conn.getBrowser();
			sourceservice.deleteRemoteSchemaOrFail(remotename);
			return JAXBSuccessResponseBuilder.getJAXBResponse("Deleted");
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

}
