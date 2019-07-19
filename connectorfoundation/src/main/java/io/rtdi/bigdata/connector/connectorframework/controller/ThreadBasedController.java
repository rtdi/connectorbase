package io.rtdi.bigdata.connector.connectorframework.controller;

import java.io.IOException;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorTemporaryException;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerExitType;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerRequestedState;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerState;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineTemporaryException;

public abstract class ThreadBasedController<C extends Controller<?>> extends Controller<C> implements Runnable {

	private Thread thread = null;
	/**
	 * The last un-recoverable exception
	 */
	protected Exception lastexception;

	public ThreadBasedController(String name) {
		super(name);
	}

	@Override
	public void startController() throws IOException {
		requestedstate = ControllerRequestedState.RUN;
		state = ControllerState.STARTING;
		startControllerImpl();
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
				thread.interrupt();
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
		if (thread == null) {
			state = ControllerState.STOPPED;
			return true;
		} else if (thread.isAlive()) {
			try {
				switch (exittype) {
				case ABORT:
					thread.join(10000);
					break;
				case ENDBATCH:
					thread.join(60000);
					break;
				case ENDROW:
					thread.join(30000);
					break;
				default:
					thread.join(30000);
					break;
				}
			} catch (InterruptedException e) {
			}
			return !thread.isAlive();
		} else {
			return true;
		}
	}

	@Override
	public boolean isRunning() {
		return super.isRunning() && 
				thread != null && 
				!thread.isInterrupted() && 
				thread.isAlive();
	}

	@Override
	public void run() {
		lastexception = null;
		try {
			logger.info("Controller thread {} started", Thread.currentThread().getName());
			while (isRunning()) {
				state = ControllerState.STARTING;
				try {
					startThreadControllerImpl();
					startChildController();
					state = ControllerState.STARTED;
					runUntilError();
					// In the normal shutdown case wait for the children to terminate and then end as well
					joinChildControllers(ControllerExitType.ENDBATCH);
				} catch (ConnectorTemporaryException e) {
					errors.addError(e);
					logger.error("Controller ran into a temporary error, retrying", e);
					state = ControllerState.TEMPORARYERROR;
				} catch (PipelineTemporaryException e) {
					if (retryPipelineTemporaryExceptions()) {
						errors.addError(e);
						logger.error("Controller ran into a temporary pipeline error, retrying", e);
						state = ControllerState.TEMPORARYERROR;
					} else {
						throw e;
					}
				} finally {
					// Failsafe to stop left over threads that did not stop without a Thread.interrupt() signal
					stopChildControllers(ControllerExitType.ABORT);
					joinChildControllers(ControllerExitType.ABORT);
					stopThreadControllerImpl(ControllerExitType.ABORT);
				}
				
			}
		} catch (Exception e) {
			lastexception = e;
			logger.error("Controller ran into a permanent error, stopping", e);
		} finally {
			stopChildControllers(ControllerExitType.ABORT);
			joinChildControllers(ControllerExitType.ABORT);
			logger.info("Controller thread {} stopped", Thread.currentThread().getName());
		}
	}

	/**
	 * The main execution loop runs until it is stopped or a child thread is no longer alive.
	 * @throws Exception if error
	 */
	protected void runUntilError() throws Exception {
		int executioncounter = 0;
		while (isRunning() && checkChildren()) {
			controllerLoop(executioncounter++);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
		}
	}
	
	/**
	 * This method is called about every 1 second and allows to add custom periodic code
	 * while the controller is running.
	 * 
	 * @param executioncounter providing the number of executions since start (within the runUntilError() method)
	 */
	protected void controllerLoop(int executioncounter) {
	}

	/*
	protected void testChildControllers() throws IOException {
		for (String childname : childcontrollers.keySet()) {
			Controller<?> t = childcontrollers.get(childname);
			if (t.getException() != null) {
				if (t.getException() instanceof IOException) {
					throw (IOException) t.getException();
				} else {
					throw new ConnectorTemporaryException("Child controller ran into an error", t.getException(), null, t.getThread().getName());
				}
			} else if (t.getRequestedState() != ControllerRequestedState.DISABLE && (t.getThread() == null || !t.getThread().isAlive())) {
				logger.info("Controller thread {} noticed that child controller thread {} is no longer alive but should", thread.getName(), t.getThread().getName());
				throw new ConnectorTemporaryException("Controller thread noticed that child controller thread is no longer alive but should", null, t.getThread().getName());
			}
		}
	}
	*/

}
