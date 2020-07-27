package io.rtdi.bigdata.connector.connectorframework.controller;

import java.io.IOException;

import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerExitType;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerState;

public abstract class ThreadBasedController<C extends Controller<?>> extends Controller<C> implements Runnable {

	private Thread thread = null;
	/**
	 * The last non-recoverable exception
	 */
	
	/**
	 * @param name Controller name
	 */
	public ThreadBasedController(String name) {
		super(name);
	}


	@Override
	protected final void startControllerImpl() {
		thread = new Thread(this);
		thread.setDaemon(true);
		thread.setName(getControllerType() + "_" + getName());
		thread.start();
	}

	/**
	 * Does everything to run this service.
	 * Child controllers are started automatically, so no need to do that.
	 * In case of a restart, this method together with {@link #stopThreadControllerImpl(ControllerExitType)} can be called multiple times
	 * within the life cycle of the thread.
	 * 
	 * @throws IOException if error
	 */
	protected abstract void startThreadControllerImpl() throws IOException;
	
	@Override
	protected final void stopControllerImpl(ControllerExitType exittype) {
		if (thread != null && thread.isAlive()) {
			logger.info("Controller thread {} to be stopped with force {}", thread.getName(), exittype.name());
			if (exittype == ControllerExitType.ABORT) {
				interrupt();
			}
		}
		stopThreadControllerImpl(exittype);
	}
	
	/**
	 * The inverse to {@link #startThreadControllerImpl()}
	 * 
	 * @param exittype ControllerExitType to tell how forceful the exit should happen
	 */
	protected abstract void stopThreadControllerImpl(ControllerExitType exittype);

	@Override
	public boolean joinAllImpl(ControllerExitType exittype) {
		if (exittype == null) {
			exittype = ControllerExitType.ABORT;
		}
		if (thread == null) {
			state = ControllerState.STOPPED;
			return true;
		} else if (thread.isAlive()) {
			logger.debug("Joining thread {} with grace period {}", thread.getName(), exittype.name());
			switch (exittype) {
			case ABORT:
				join(10000);
				break;
			default:
				join(30000);
				break;
			}
			return !thread.isAlive();
		} else {
			return true;
		}
	}

	@Override
	public boolean isRunning() {
		return (state == ControllerState.STARTED  || state == ControllerState.STARTING) &&
				thread != null && 
				!thread.isInterrupted() &&
				thread.isAlive();
	}

	@Override
	public void run() {
		try {
			lastalive = System.currentTimeMillis();
			logger.info("Controller thread {} started", Thread.currentThread().getName());
			startThreadControllerImpl(); // start this thread
			state = ControllerState.STARTED;
			startChildController();
			runUntilError();
		} catch (Exception e) {
			errors.addError(e);
			logger.error("Controller ran into a permanent error, stopping", e);
		} finally {
			stopChildControllers(ControllerExitType.ABORT);
			joinChildControllers(ControllerExitType.ABORT);
			stopThreadControllerImpl(ControllerExitType.ABORT);
			logger.info("Controller thread {} stopped", Thread.currentThread().getName());
			lastalive = System.currentTimeMillis();
		}
	}

	/**
	 * The main execution loop runs until it is stopped.
	 * @throws Exception if error
	 */
	protected abstract void runUntilError() throws Exception;
	
	public void interrupt() {
		if (thread != null) {
			thread.interrupt();
		}
	}

	public void join(long millis) {
		if (thread != null) {
			try {
				thread.join(millis);
			} catch (InterruptedException e) {
				interrupt();
			}
		}
	}

	public void sleep(long millis) {
		if (thread != null) {
			try {
				Thread.sleep(millis);
			} catch (InterruptedException e) {
				interrupt();
			}
		}
	}

}
