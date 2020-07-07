package io.rtdi.bigdata.connector.connectorframework.exceptions;

public class ShutdownException extends ConnectorTemporaryException {

	private static final long serialVersionUID = -388537069846925103L;

	public ShutdownException(String message, String causingobject) {
		super(message, null, null, causingobject);
	}

}
