package io.rtdi.bigdata.fileconnector;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;

public class FileConnectionProperties extends ConnectionProperties {
	public static final String ROOT_DIRECTORY = "file.directory";

	public FileConnectionProperties(String name) {
		super(name);
		properties.addStringProperty(ROOT_DIRECTORY, "Root Directory", "The root directory of the files", "sap-icon://target-group", null, false);
	}

	public String getRootDirectory() throws PropertiesException {
		return properties.getStringPropertyValue(ROOT_DIRECTORY);
	}
	
	public void setRootDirectory(String dir) throws PropertiesException {
		properties.setProperty(ROOT_DIRECTORY, dir);
	}

}
