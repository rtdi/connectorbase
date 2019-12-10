package io.rtdi.bigdata.connector.connectorframework.controller;

import java.io.IOException;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorTemporaryException;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerExitType;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerRequestedState;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerState;

public abstract class ThreadBasedController<C extends Controller<?>> extends Controller<C> implements Runnable {

	private Thread thread = null;
	protected boolean interruptedflag = false;
	/**
	 * The last un-recoverable exception
	 */
	protected Exception lastexception;

	public ThreadBasedController(String name) {
		super(name);
	}

	@Override
	public void startController(boolean force) throws IOException {
		requestedstate = ControllerRequestedState.RUN;
		state = ControllerState.STARTING;
		interruptedflag = false;
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
				setLastException(e);
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
				!thread.isInterrupted() && !interruptedflag &&
				thread.isAlive();
	}

	@Override
	public void run() {
		lastexception = null;
		try {
			logger.info("Controller thread {} started", Thread.currentThread().getName());
			startThreadControllerImpl(); // start this thread
			while (isRunning()) {
				state = ControllerState.STARTING;
				try {
					startChildController(false);
					state = ControllerState.STARTED;
					runUntilError();
					// In the normal shutdown case wait for the children to terminate and then end as well
					joinChildControllers(ControllerExitType.ENDBATCH);
				} catch (ConnectorTemporaryException e) {
					errors.addError(e);
					logger.error("Controller ran into a temporary error, retrying", e);
					state = ControllerState.TEMPORARYERROR;
				} finally {
					// Failsafe to stop left over threads that did not stop without a Thread.interrupt() signal
					stopChildControllers(ControllerExitType.ABORT);
					joinChildControllers(ControllerExitType.ABORT);
				}
				
			}
		} catch (Exception e) {
			lastexception = e;
			logger.error("Controller ran into a permanent error, stopping", e);
		} finally {
			stopChildControllers(ControllerExitType.ABORT);
			joinChildControllers(ControllerExitType.ABORT);
			stopThreadControllerImpl(ControllerExitType.ABORT);
			logger.info("Controller thread {} stopped", Thread.currentThread().getName());
		}
	}

	/**
	 * The main execution loop runs until it is stopped or a child thread is no longer alive.
	 * @throws Exception if error
	 */
	protected void runUntilError() throws Exception {
		long executioncounter = 0;
		while (isRunning() && checkChildren()) {
			if (isRunning()) {
				periodictask(executioncounter);
				executioncounter++;
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					setLastException(e);
					return;
				}
			}
		}
	}
	
	/**
	 * This method is called about every 1 second and allows to add custom periodic code
	 * while the controller is running.
	 * 
	 * @param executioncounter providing the number of executions since start (within the runUntilError() method)
	 */
	protected void periodictask(long executioncounter) {
	}

	public void setLastException(Exception e) {
		this.lastexception = e;
		if (e instanceof InterruptedException) {
			interruptedflag = true;
		}
	}

}
