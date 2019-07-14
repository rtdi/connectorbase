package io.rtdi.bigdata.connector.pipeline.foundation.exceptions;

/**
 * Problems related to the Avro schema
 *
 */
public class SchemaException extends Exception {
	private static final long serialVersionUID = -4072142761605595025L;

	public SchemaException() {
		super();
	}

	/**
	 * Note that {@link #ConnectorException(String, ErrorOrigin)} is preferred
	 * 
	 * @param message Any English error text
	 */
	public SchemaException(String message) {
		super(message);
	}

	/**
	 * Note that {@link #ConnectorException(String, ErrorOrigin)} is preferred
	 * 
	 * @param cause
	 */
	public SchemaException(Throwable cause) {
		super(cause);
	}

	/**
	 * Note that {@link #ConnectorException(String, ErrorOrigin)} is preferred
	 * 
	 * @param message
	 * @param cause
	 */
	public SchemaException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Note that {@link #ConnectorException(String, ErrorOrigin)} is preferred
	 * 
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public SchemaException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

}
