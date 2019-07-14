package io.rtdi.bigdata.connector.connectorframework.exceptions;

public class ConnectorCallerException extends ConnectorTemporaryException {

	private static final long serialVersionUID = -2298633421478144454L;

	public ConnectorCallerException() {
		super();
	}

	public ConnectorCallerException(String message, String hint, String causingobject) {
		super(message, hint, causingobject);
	}

	public ConnectorCallerException(String message, Throwable cause, String hint, String causingobject) {
		super(message, cause, hint, causingobject);
	}

	public ConnectorCallerException(String message) {
		super(message);
	}

	public ConnectorCallerException(Throwable cause) {
		super(cause);
	}

}
