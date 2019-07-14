package io.rtdi.bigdata.connector.pipeline.foundation.entity;

public class JAXBErrorMessage {
	private String errortext;
	private String stacktrace;
	
	public JAXBErrorMessage(Throwable e) {
		super();
		errortext = e.getMessage();
		stacktrace = ErrorListEntity.getStackTrace(e);
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

}
