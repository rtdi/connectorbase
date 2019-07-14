package io.rtdi.bigdata.connector.pipeline.foundation.exceptions;


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
		super(message, cause, hint, null);
	}
	
	public PipelineRuntimeException(String message, Throwable cause, String hint, String causingobject) {
		super(message, cause, hint, causingobject);
	}
	
}
