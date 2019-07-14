package io.rtdi.bigdata.connector.pipeline.foundation.enums;

/**
 * A container can be started/stopped or half way with some instances running, others not.
 *
 */
public enum ControllerState {
	/**
	 * The Listener has no running threads.
	 */
	STOPPED,
	/**
	 * The Listener has all instances running.
	 */
	STARTED,
	/**
	 * The Listener is in the process of stopping.
	 */
	STOPPING,
	/**
	 * The Listener is in the process of starting.
	 */
	STARTING,
	TEMPORARYERROR
}
