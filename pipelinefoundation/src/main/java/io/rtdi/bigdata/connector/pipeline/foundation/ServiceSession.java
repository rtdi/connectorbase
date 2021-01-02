package io.rtdi.bigdata.connector.pipeline.foundation;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.connector.pipeline.foundation.entity.OperationLogContainer.StateDisplayEntry;
import io.rtdi.bigdata.connector.properties.ServiceProperties;

public abstract class ServiceSession {	
	protected Logger logger = LogManager.getLogger(this.getClass().getName());
	private ServiceProperties properties;

	public ServiceSession(ServiceProperties properties) {
		this.properties = properties;
	}

	public abstract void start() throws IOException;
	
	public abstract void stop();
	
	public long getRowsProcessed() {
		if (properties.getMicroserviceTransformations() != null) {
			long rowcount = 0L;
			for (MicroServiceTransformation transformations : properties.getMicroserviceTransformations()) {
				rowcount += transformations.getRowProcessed();
			}
			return rowcount;
		} else {
			return 0;
		}
	}

	public Long getLastRowProcessed() {
		if (properties.getMicroserviceTransformations() != null) {
			long maxdate = 0L;
			for (MicroServiceTransformation transformations : properties.getMicroserviceTransformations()) {
				if (transformations != null) {
					Long d = transformations.getLastRowProcessed();
					if (d != null && d > maxdate) {
						maxdate = d;
					}
				}
			}
			return maxdate;
		} else {
			return null;
		}
	}
	
	public Map<String, List<StateDisplayEntry>> getMicroserviceOperationLogs() {
		if (properties.getMicroserviceTransformations() != null) {
			Map<String, List<StateDisplayEntry>> ret = new HashMap<>();
			for (MicroServiceTransformation transformations : properties.getMicroserviceTransformations()) {
				ret.put(transformations.getName(), transformations.getOperationLog().asList());
			}
			return ret;
		} else {
			return null;
		}
	}

	public ServiceProperties getProperties() {
		return properties;
	}
	
}
