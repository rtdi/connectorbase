package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedList;
import java.util.List;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class ErrorListEntity {
	private LinkedList<ErrorEntity> errors = new LinkedList<>();
	private int capacity = 10;

	public ErrorListEntity() {
	}

	public ErrorListEntity(int capacity) {
		this.capacity = capacity;
	}

	public void addError(ErrorEntity error) {
		errors.add(error);
		while (errors.size() > capacity) {
			errors.remove();
		}
	}
	
	public void addError(String message, String exception, String stacktrace, String hint, String causingobject, String sourcecodeline, String errorhelp) {
		ErrorEntity error = new ErrorEntity(message, exception, stacktrace, hint, causingobject, sourcecodeline, errorhelp);
		addError(error);
	}
	
	public void addError(PropertiesException e) {
		addError(e, e.getHint(), e.getCausingObject(), e.getSourceCodeLink(), e.getErrorHelp());
	}
	
	public void addError(Exception e, String hint, String causingobject, String sourcecodeline, String errorhelp) {
		addError(e.getMessage(), e.getClass().getSimpleName(), getStackTrace(e), hint, causingobject, sourcecodeline, errorhelp);
	}

	public void addError(Exception e, String hint, String causingobject) {
		addError(e.getMessage(), e.getClass().getSimpleName(), getStackTrace(e), hint, causingobject, null, null);
	}

	public List<ErrorEntity> getErrors() {
		return errors;
	}

	public static String getStackTrace(Throwable e) {
		StringWriter s = new StringWriter();
		PrintWriter w = new PrintWriter(s);
		e.printStackTrace(w);
		return s.toString();
	}

}
