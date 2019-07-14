package io.rtdi.bigdata.connector.pipeline.foundation;

import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerState;

public interface IControllerState {

	/**
	 * @return ControllerState of the controller to know if it is running, stopped etc
	 */
	ControllerState getState();
	
	/**
	 * @return true if the controller should continue running, false in case a stop was requested
	 */
	boolean isRunning();

}