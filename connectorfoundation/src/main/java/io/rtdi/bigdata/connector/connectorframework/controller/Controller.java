package io.rtdi.bigdata.connector.connectorframework.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.IControllerState;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ErrorEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ErrorListEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerExitType;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerState;


/**
 * An abstract class implementing the common logic for all controllers.<br>
 * A controller is a robust implementation that tries to keep its children running, provides
 * monitoring information, restarts automatically etc.
 *
 * @param <C> The concrete type of the child Controller
 */
public abstract class Controller<C extends Controller<?>> implements IControllerState {
	protected final Logger logger;
	protected ControllerState state = ControllerState.STOPPED;
	protected HashMap<String, C> childcontrollers = new HashMap<>();
	private String name;
	protected ErrorListEntity errors = new ErrorListEntity();
	protected boolean controllerdisabled = false;
	protected long lastalive = System.currentTimeMillis();
	
	public Controller(String name) {
		super();
		this.name = name;
		logger = LogManager.getLogger(this.getClass().getName());
	}

	/**
	 * Initializes and starts the controller.
	 * Also removes the disabled flag.
	 * 
	 * @throws IOException if the controller or one of its children cannot be started 
	 */
	public synchronized void startController() throws IOException {
		if (state != ControllerState.STARTING && state != ControllerState.STARTED) {
			lastalive = System.currentTimeMillis();
			controllerdisabled = false;
			errors = new ErrorListEntity();
			state = ControllerState.STARTING;
			startControllerImpl();
			state = ControllerState.STARTED;
		} else {
			logger.info("A request to start a controller that is started/starting already was made - ignored");
		}
	}

	protected void startChildController() throws IOException {
		for (Controller<?> c : childcontrollers.values()) {
			if (!c.isControllerDisabled()) {
				c.startController();
			}
		}
	}
	
	/**
	 * @return true in case this controller is temporarily disabled
	 */
	public boolean isControllerDisabled() {
		return controllerdisabled;
	}
	
	/**
	 * Called to temporarily stop the Controller and prevent it from being recovered.
	 */
	public void disableController() {
		this.controllerdisabled = true;
		this.stopController(ControllerExitType.ABORT);
		this.joinAll(ControllerExitType.ABORT);
	}

	protected abstract void updateLandscape();
	
	protected abstract void updateSchemaCache();

	/**
	 * @return name of the controller as provided in the constructor; usually the same as the properties name
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * @return implementers return the name of the controller type; used to set the thread name
	 */
	protected abstract String getControllerType();
	
	/**
	 * Signal this controller to stop and does stop all child controllers indirectly
	 * 
	 * @param exittype ControllerExitType to tell how forceful the exit should happen
	 */
	public void stopController(ControllerExitType exittype) {
		state = ControllerState.STOPPING;
		stopControllerImpl(exittype);
		state = ControllerState.STOPPED;
	}

	/**
	 * Signal all child controllers to stop
	 * 
	 * @param exittype ControllerExitType to tell how forceful the exit should happen
	 */
	protected void stopChildControllers(ControllerExitType exittype) {
		for (Controller<?> c : childcontrollers.values()) {
			c.stopController(exittype);
		}
	}

	/**
	 * Wait for all child controllers to terminate to avoid error messages about still running threads during their termination.
	 * In case the controller is not a thread on its own this means simply to set the state to STOPPED and to trigger the child controllers to stop.
	 * 
	 * @param exittype ControllerExitType to tell how forceful the exit should happen
	 * @return true if all children and this controller have been stopped successfully within a time depending on the exittype
	 */
	public boolean joinAll(ControllerExitType exittype) {
		boolean allstopped = joinAllImpl(exittype);
		allstopped &= joinChildControllers(exittype);
		state = ControllerState.STOPPED;
		return allstopped;
	}

	/**
	 * In case the controller needs to do something extra during the wait, it can be implemented here. Example is the thread base controller
	 * waiting for the thread to actually terminate.
	 * 
	 * @param exittype ControllerExitType to tell how forceful the exit should happen
	 * @return true id the controller was stopped successfully
	 */
	protected boolean joinAllImpl(ControllerExitType exittype) {
		return true;
	}

	/**
	 * @param exittype ControllerExitType to tell how forceful the exit should happen
	 * @return true if all children have been stopped successfully within a time depending on the exittype
	 */
	protected boolean joinChildControllers(ControllerExitType exittype) {
		boolean allstopped = true;
		for (Controller<?> c : childcontrollers.values()) {
			allstopped = allstopped & c.joinAll(exittype);
		}
		return allstopped;
	}

	/**
	 * Allows the individual controller implementations to execute own code at start.
	 * 
	 * @throws IOException if error
	 */
	protected abstract void startControllerImpl() throws IOException;
	
	/**
	 * Allows the individual controller implementations to execute own code at stop.
	 * 
	 * @param exittype ControllerExitType to tell how forceful the exit should happen
	 */
	protected void stopControllerImpl(ControllerExitType exittype) {
		stopChildControllers(exittype);
	}

	/**
	 * All child controllers are added so the {@link #stopController(ControllerExitType)} knows the children to stop.
	 * 
	 * @param name of the child
	 * @param controller instance to add
	 * @throws ConnectorRuntimeException if a child controller of same name exists 
	 */
	public void addChild(String name, C controller) throws ConnectorRuntimeException {
		C currentcontroller = childcontrollers.get(name);
		if (currentcontroller != null) {
			/*
			 * If the child of that name exists already, remove it.
			 * Might happen if a parent reloads the configuration which does impact the children as well
			 */
			currentcontroller.disableController();
		}
		childcontrollers.put(name, controller);
	}
	
	/**
	 * @return list of recent errors
	 */
	public List<ErrorEntity> getErrorList() {
		return errors.getErrors();
	}

	/**
	 * @return list of recent errors
	 */
	public List<ErrorEntity> getErrorListRecursive() {
		List<ErrorEntity> l = new ArrayList<>();
		l.addAll(errors.getErrors());
		if (getChildControllers() != null) {
			for (C c : getChildControllers().values()) {
				l.addAll(c.getErrorListRecursive());
			}
		}
		return l;
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.pipeline.foundation.IControllerState#isRunning()
	 */
	@Override
	public boolean isRunning() {
		return (state != ControllerState.STOPPED);
	}

	/**
	 * Checks if all children are running and starts them in case
	 * @throws IOException if any of the children have a problem
	 */
	public void checkChildren() throws IOException {
		for (String childname : childcontrollers.keySet()) {
			Controller<?> t = childcontrollers.get(childname);
			if (!t.isControllerDisabled()) {
				if (!t.isRunning()) {
					// Child is not running, maybe it halted a second ago?
					if (System.currentTimeMillis() - t.getLastAlive() > 60000) {
						t.stopController(ControllerExitType.ABORT); // bring down the thread completely in case it is currently starting
						t.startController();
					}
				} else {
					t.checkChildren();
				}
			}
		}
	}

	private long getLastAlive() {
		return lastalive ;
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.connectorframework.controller.IControllerState#getState()
	 */
	@Override
	public ControllerState getState() {
		return state;
	}
	
	@Override
	public String toString() {
		return getControllerType() + " for " + getName();
	}

	/**
	 * @return all registered child controllers
	 */
	protected HashMap<String, C> getChildControllers() {
		return childcontrollers;
	}
	
	/**
	 * @param name Name of the child controller to look for
	 * @return the Controller of that name
	 */
	public C getChildController(String name) {
		return childcontrollers.get(name);
	}
	
	public void addError(Exception e) {
		errors.addError(e);
	}

}
