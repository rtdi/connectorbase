package io.rtdi.bigdata.connector.connectorframework.exceptions;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class ConnectorRuntimeException extends PropertiesException {

	private static final long serialVersionUID = -6486968192914820208L;

	public ConnectorRuntimeException() {
		super();
	}

	public ConnectorRuntimeException(String message, String hint, String causingobject) {
		super(message, hint, causingobject);
	}

	public ConnectorRuntimeException(String message, Throwable cause, String hint, String causingobject) {
		super(message, cause, hint, causingobject);
	}

	public ConnectorRuntimeException(String message) {
		super(message);
	}

	public ConnectorRuntimeException(Throwable cause) {
		super(cause);
	}

}
