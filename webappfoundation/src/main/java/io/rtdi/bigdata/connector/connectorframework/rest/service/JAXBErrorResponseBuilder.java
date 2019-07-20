package io.rtdi.bigdata.connector.connectorframework.rest.service;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import io.rtdi.bigdata.connector.pipeline.foundation.entity.JAXBErrorMessage;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class JAXBErrorResponseBuilder {
	
	public static Response getJAXBResponse(Throwable e) {
		if (e instanceof PropertiesException) {
			PropertiesException a = (PropertiesException) e;
			return Response.status(mapErrorOrigin(a)).entity(new JAXBErrorMessage(e)).build();
		} else {
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(new JAXBErrorMessage(e)).build();
		}
	}

	public static Response getJAXBResponse(String string) {
		return Response.status(Status.BAD_REQUEST).entity(new JAXBErrorMessage(string)).build();
	}

	private static Status mapErrorOrigin(PropertiesException e) {
		return Status.INTERNAL_SERVER_ERROR;
		/*
		switch (e.getErrorOrigin()) {
		case CALLER:
			return Status.BAD_REQUEST;
		case PROGRAMMINGERROR:
			return Status.NOT_IMPLEMENTED;
		case SERVERSTATE:
			return Status.INTERNAL_SERVER_ERROR;
		case SERVERTEMPORARY:
			return Status.SERVICE_UNAVAILABLE;
		case UNKNOWN:
			return Status.INTERNAL_SERVER_ERROR;
		default:
			return Status.INTERNAL_SERVER_ERROR;
		} */
	}
}
