package io.rtdi.bigdata.connector.pipeline.foundation.exceptions;

/**
 * Not out fault, the caller did something illegal.
 *
 */
public class PipelineCallerException extends PipelineTemporaryException {

	private static final long serialVersionUID = -6210537140205590102L;

	public PipelineCallerException() {
		super();
	}

	public PipelineCallerException(String message) {
		super(message);
	}

	public PipelineCallerException(String message, String hint) {
		super(message, hint);
	}

	public PipelineCallerException(String message, Throwable cause, String hint) {
		super(message, cause, hint);
	}

}
