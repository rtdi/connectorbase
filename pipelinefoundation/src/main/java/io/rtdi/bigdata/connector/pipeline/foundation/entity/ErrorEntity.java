package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class ErrorEntity {

	private long timestamp;
	protected String message;
	private String exception;
	private String stacktrace;
	private String hint;
	private String causingobject;
	private String sourcecodeline;
	private String errorhelp;
	private String threadname;
	
	public ErrorEntity() {
		super();
		timestamp = System.currentTimeMillis();
	}
	
	public ErrorEntity(String message, String exception, String stacktrace, String hint, String causingobject, String sourcecodeline, String errorhelp) {
		this();
		this.message = message;
		this.exception = exception;
		this.stacktrace = stacktrace;
		this.hint = hint;
		this.causingobject = causingobject;
		this.sourcecodeline = sourcecodeline;
		this.errorhelp = errorhelp;
		this.threadname = Thread.currentThread().getName();
	}
	
	public ErrorEntity(Throwable e) {
		this();
		message = e.getMessage();
		stacktrace = ErrorListEntity.getStackTrace(e);
		exception = e.getClass().getSimpleName();
		this.threadname = Thread.currentThread().getName();
		if (e instanceof PropertiesException) {
			PropertiesException pe = (PropertiesException) e;
			errorhelp = pe.getErrorHelp();
			sourcecodeline = pe.getSourceCodeLink();
			hint = pe.getHint();
			causingobject = pe.getCausingObject();
		}
	}

	
	public long getTimestamp() {
		return timestamp;
	}

	public String getMessage() {
		return message;
	}

	public String getException() {
		return exception;
	}

	public String getStacktrace() {
		return stacktrace;
	}

	public String getHint() {
		return hint;
	}

	public String getCausingobject() {
		return causingobject;
	}

	public String getSourcecodeline() {
		return sourcecodeline;
	}

	public String getErrorhelp() {
		return errorhelp;
	}
	
	public String getThreadname() {
		return threadname;
	}

}
