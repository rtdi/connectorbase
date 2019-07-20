package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class JAXBErrorMessage {
	private String errortext;
	private String stacktrace;
	private String errorhelp;
	private String sourcecodeline;
	
	public JAXBErrorMessage(Throwable e) {
		super();
		errortext = e.getMessage();
		stacktrace = ErrorListEntity.getStackTrace(e);
		if (e instanceof PropertiesException) {
			PropertiesException pe = (PropertiesException) e;
			errorhelp = pe.getErrorHelp();
			sourcecodeline = pe.getSourceCodeLink();
		}
	}
		
	public JAXBErrorMessage() {
		super();
	}
	
	public JAXBErrorMessage(String errortext) {
		super();
		this.errortext = errortext;
	}

	public String getErrortext() {
		return errortext;
	}
	
	public void setErrortext(String errortext) {
		this.errortext = errortext;
	}
	
	public String getStacktrace() {
		return stacktrace;
	}
	
	public void setStacktrace(String stacktrace) {
		this.stacktrace = stacktrace;
	}

	public String getHelpText() {
		return errorhelp;
	}

	public void setHelpText(String errorhelp) {
		this.errorhelp = errorhelp;
	}

	public String getSourcecodeLine() {
		return sourcecodeline;
	}

	public void setSourcecodeLine(String sourcecodeline) {
		this.sourcecodeline = sourcecodeline;
	}

}
