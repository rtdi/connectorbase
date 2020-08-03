package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedList;
import java.util.List;

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
	
	public void addError(Exception e) {
		addError(new ErrorEntity(e));
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

	public static String getStackTraceRootCause(Throwable e) {
		Throwable cause = e.getCause();
		if (cause == null) {
			return null;
		} else {
			while (cause.getCause() != null) {
				cause = cause.getCause();
			}
			return getStackTrace(cause);
		}
	}

}
