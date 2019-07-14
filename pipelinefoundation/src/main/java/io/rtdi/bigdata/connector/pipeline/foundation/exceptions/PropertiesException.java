package io.rtdi.bigdata.connector.pipeline.foundation.exceptions;

import java.io.IOException;

/**
 * Something wrong with the properties.
 *
 */
public class PropertiesException extends IOException {

	private static final long serialVersionUID = 116063643000518740L;
	private String hint = null;
	private String causingobject = null;

	public PropertiesException() {
		super();
	}

	public PropertiesException(String message) {
		super(message);
	}

	public PropertiesException(Throwable cause) {
		super(cause);
	}

	public PropertiesException(String message, String hint, String causingobject) {
		super(message);
		this.hint = hint;
		this.causingobject = causingobject;
	}

	public PropertiesException(String message, Throwable cause, String hint) {
		this(message, cause, hint, null);
	}
	
	public PropertiesException(String message, String hint) {
		this(message, hint, null);
	}

	public PropertiesException(String message, Throwable cause) {
		this(message, cause, null, null);
	}

	/**
	 * The preferred constructor, where an error message, the causing exception, a hint of what the user can do and the object causing the problem.
	 * 
	 * @param message as free form text
	 * @param cause is the causing exception, optional
	 * @param hint provides optional guidance to the user
	 * @param causingobject identifies the input causing the exception
	 */
	public PropertiesException(String message, Throwable cause, String hint, String causingobject) {
		super(message, cause);
		this.hint = hint;
		this.causingobject = causingobject;
	}

	public String getHint() {
		return hint;
	}

	public String getCausingObject() {
		return causingobject;
	}

}
