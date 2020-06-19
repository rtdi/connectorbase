package io.rtdi.bigdata.connector.pipeline.foundation.enums;

/**
 * 
 *
 */
public enum ControllerExitType {
	/**
	 * The thread is running 
	 */
	RUN,
	/**
	 * Let the batch operation finish - the least invasive stop
	 */
	ENDBATCH,
	/**
	 * Let the record finish but break the batch.
	 */
	ENDROW,
	/**
	 * Stop immediately at all costs, use thread.interrupt().
	 */
	ABORT
}
