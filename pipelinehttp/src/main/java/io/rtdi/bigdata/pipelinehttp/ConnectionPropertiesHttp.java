package io.rtdi.bigdata.pipelinehttp;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.PipelineConnectionProperties;

public class ConnectionPropertiesHttp extends PipelineConnectionProperties {

	private static final String URL = "http.url";
	private static final String USER = "http.user";
	private static final String PASSWORD = "http.password";

	public ConnectionPropertiesHttp(String name) {
		super(name);
		properties.addStringProperty(URL, "Target URL", "The URL of the corresponding ConnectionServer", null, null, false);
		properties.addStringProperty(USER, "ConnectionServer user name", "Username for the authentication", null, null, false);
		properties.addPasswordProperty(PASSWORD, "ConnectionServer password", "Password for the authentication", null, null, false);
	}

	public String getAdapterServerURI() {
		return properties.getStringPropertyValue(URL);
	}

	public void setAdapterServerURI(String url) throws PropertiesException {
		properties.setProperty(URL, url);
	}

	public String getUser() {
		return properties.getStringPropertyValue(USER);
	}

	public void setUser(String user) throws PropertiesException {
		properties.setProperty(USER, user);
	}

	public String getPassword() {
		return properties.getPasswordPropertyValue(PASSWORD);
	}

	public void setPassword(String password) throws PropertiesException {
		properties.setProperty(PASSWORD, password);
	}
}
