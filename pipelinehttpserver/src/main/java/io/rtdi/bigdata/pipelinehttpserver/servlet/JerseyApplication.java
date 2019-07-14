package io.rtdi.bigdata.pipelinehttpserver.servlet;

import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;


public class JerseyApplication extends ResourceConfig {
	public static final String DEFAULT_ROLE = "pipelineuser";
	
	public JerseyApplication() {
		super();
		packages("io.rtdi.bigdata.pipelinehttpserver.service");
		register(JacksonFeature.class);
	}

	protected String[] getPackages() {
		return null;
	}
}