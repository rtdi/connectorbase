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
	private String stacktracerootcause;
	
	public ErrorEntity() {
		super();
		timestamp = System.currentTimeMillis();
	}
	
	public ErrorEntity(Throwable e) {
		this();
		message = e.getMessage();
		stacktrace = ErrorListEntity.getStackTrace(e);
		stacktracerootcause = ErrorListEntity.getStackTraceRootCause(e);
		exception = e.getClass().getSimpleName();
		this.threadname = Thread.currentThread().getName();
		if (e instanceof PropertiesException) {
			PropertiesException pe = (PropertiesException) e;
			errorhelp = null;
			sourcecodeline = pe.getSourceCodeLink();
			hint = pe.getHint();
			causingobject = pe.getCausingObject();
		}
		if (message == null) {
			message = exception;
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

	public String getStacktracerootcause() {
		return stacktracerootcause;
	}

}
