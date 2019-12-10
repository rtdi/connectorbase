package io.rtdi.bigdata.connector.pipeline.foundation;

import java.io.IOException;

public abstract class ServiceSession {	
	
	public abstract void start() throws IOException;
	
	public abstract void stop();
	
	public abstract long getRowsProcessed();
	
	public abstract Long getLastRowProcessed();
	
}
