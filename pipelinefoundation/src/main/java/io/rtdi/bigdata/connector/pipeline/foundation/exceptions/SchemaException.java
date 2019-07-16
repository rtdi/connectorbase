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
	 * 
	 * @param message as English error text
	 */
	public SchemaException(String message) {
		super(message);
	}

	/**
	 * 
	 * @param cause why this happened
	 */
	public SchemaException(Throwable cause) {
		super(cause);
	}

	/**
	 * 
	 * @param message as English error text
	 * @param cause why this happened
	 */
	public SchemaException(String message, Throwable cause) {
		super(message, cause);
	}

}
