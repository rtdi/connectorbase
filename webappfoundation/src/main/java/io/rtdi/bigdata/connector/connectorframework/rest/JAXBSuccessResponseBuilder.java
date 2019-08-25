package io.rtdi.bigdata.connector.connectorframework.rest;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import io.rtdi.bigdata.connector.pipeline.foundation.entity.JAXBSuccessMessage;

public class JAXBSuccessResponseBuilder {
	
	public static Response getJAXBResponse(String text) {
		return Response.status(Status.OK).entity(new JAXBSuccessMessage(text)).build();
	}
}
