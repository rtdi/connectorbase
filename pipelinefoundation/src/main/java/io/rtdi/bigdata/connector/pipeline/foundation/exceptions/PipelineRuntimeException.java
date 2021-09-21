package io.rtdi.bigdata.connector.pipeline.foundation.exceptions;

import java.util.concurrent.ExecutionException;

/**
 * An Exception where a retry does not make sense. Something severe happened within the code and
 * it will happen again.
 *
 */
public class PipelineRuntimeException extends PropertiesRuntimeException {

	private static final long serialVersionUID = 4603687392740105590L;

	public PipelineRuntimeException() {
		super();
	}

	public PipelineRuntimeException(String message) {
		super(message);
	}

	public PipelineRuntimeException(String message, String hint) {
		super(message, hint, null);
	}

	public PipelineRuntimeException(String message, Throwable cause, String hint) {
		// In case the cause is an ExecutionException, don't show it but its actual cause
		super(message, getError(cause), hint, null);
	}
	
	public PipelineRuntimeException(String message, Throwable cause, String hint, String causingobject) {
		super(message, getError(cause), hint, causingobject);
	}
	
	private static Throwable getError(Throwable cause) {
		if (cause == null) {
			return null;
		} else if (cause.getCause() == null) {
			return cause;
		} else if (cause.getCause() instanceof ExecutionException) {
			return cause.getCause();
		} else {
			return cause;
		}
	}
	
}
