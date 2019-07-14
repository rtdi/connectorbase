package io.rtdi.bigdata.connector.pipeline.foundation.exceptions;

/**
 * The properties are wrong, cannot start.
 *
 */
public class PropertiesRuntimeException extends PropertiesException {

	private static final long serialVersionUID = 47520475027502L;

	public PropertiesRuntimeException() {
		super();
	}

	public PropertiesRuntimeException(String message) {
		super(message);
	}

	public PropertiesRuntimeException(String message, String hint, String causingobject) {
		super(message, hint, causingobject);
	}

	public PropertiesRuntimeException(Throwable cause) {
		super(cause.getMessage(), cause, null, null);
	}
	
	public PropertiesRuntimeException(String message, Throwable cause, String hint, String causingobject) {
		super(message, cause, hint, causingobject);
	}
		
}
