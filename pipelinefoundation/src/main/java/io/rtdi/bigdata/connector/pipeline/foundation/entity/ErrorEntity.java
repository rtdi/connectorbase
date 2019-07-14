package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import javax.xml.bind.annotation.XmlElement;

public class ErrorEntity {

	private long timestamp;
	private String message;
	private String exception;
	private String stacktrace;
	private String hint;
	private String causingobject;
	
	public ErrorEntity() {
		super();
		timestamp = System.currentTimeMillis();
	}
	
	public ErrorEntity(String message, String exception, String stacktrace, String hint, String causingobject) {
		this();
		this.message = message;
		this.exception = exception;
		this.stacktrace = stacktrace;
		this.hint = hint;
		this.causingobject = causingobject;
	}
	
	@XmlElement
	public long getTimestamp() {
		return timestamp;
	}

	@XmlElement
	public String getMessage() {
		return message;
	}

	@XmlElement
	public String getException() {
		return exception;
	}

	@XmlElement
	public String getStacktrace() {
		return stacktrace;
	}

	@XmlElement
	public String getHint() {
		return hint;
	}

	@XmlElement
	public String getCausingObject() {
		return causingobject;
	}

}
