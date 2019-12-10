package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class JAXBErrorMessage {
	private String errortext;
	private String stacktrace;
	private String errorhelp;
	private String sourcecodeline;
	private String hint;
	private String causingobject;
	
	public JAXBErrorMessage(Throwable e) {
		super();
		errortext = e.getMessage();
		stacktrace = ErrorListEntity.getStackTrace(e);
		if (e instanceof PropertiesException) {
			PropertiesException pe = (PropertiesException) e;
			errorhelp = pe.getErrorHelp();
			sourcecodeline = pe.getSourceCodeLink();
			hint = pe.getHint();
			causingobject = pe.getCausingObject();
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

	public String getHint() {
		return hint;
	}

	public void setHint(String hint) {
		this.hint = hint;
	}

	public String getCausingobject() {
		return causingobject;
	}

	public void setCausingobject(String causingobject) {
		this.causingobject = causingobject;
	}

	public String getMarkup() {
		StringBuffer b = new StringBuffer();
		if (hint != null) {
			b.append("<h3>Hint</h3><p>");
			b.append(hint);
			b.append("</p>");
		}
		if (causingobject != null) {
			b.append("<h3>Object in question</h3><p>");
			b.append(causingobject);
			b.append("</p>");
		}
		if (errorhelp != null) {
			b.append("<h3>Error help</h3><p>");
			b.append(errorhelp);
			b.append("</p>");
		}
		if (sourcecodeline != null) {
			b.append("<h3>Sourcecode</h3><p>");
			b.append(sourcecodeline);
			b.append("</p>");
		}
		return b.toString();
	}
}
