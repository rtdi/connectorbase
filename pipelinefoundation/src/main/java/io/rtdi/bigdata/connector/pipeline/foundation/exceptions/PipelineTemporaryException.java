package io.rtdi.bigdata.connector.pipeline.foundation.exceptions;

/**
 * This is the weakest Exception, one that should be retried. This should be used for all kinds of intermittent problems.
 *
 */
public class PipelineTemporaryException extends PipelineRuntimeException {

	private static final long serialVersionUID = 1160636943000518740L;

	public PipelineTemporaryException() {
		super();
	}

	public PipelineTemporaryException(String message) {
		super(message);
	}

	public PipelineTemporaryException(String message, String hint) {
		super(message, hint);
	}

	public PipelineTemporaryException(String message, Throwable cause, String hint) {
		super(message, cause, hint);
	}

	public PipelineTemporaryException(String message, Throwable cause, String hint, String causingobject) {
		super(message, cause, hint, causingobject);
	}

}
