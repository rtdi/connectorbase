package io.rtdi.bigdata.connector.pipeline.foundation;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class ServiceSession {	
	protected Logger logger = LogManager.getLogger(this.getClass().getName());

	public abstract void start() throws IOException;
	
	public abstract void stop();
	
	public abstract long getRowsProcessed();
	
	public abstract Long getLastRowProcessed();
	
}
