package io.rtdi.bigdata.fileconnector;

import java.io.File;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ProducerProperties;

public class FileProducerProperties extends ProducerProperties {

	private static final String FILESCHEMA = "producer.fileschema";
	private static final String POLLINTERVAL = "producer.pollinterval";
	private static final String TARGETTOPIC = "producer.topic";

	public FileProducerProperties(String name) throws PropertiesException {
		super(name);
		properties.addStringProperty(FILESCHEMA, "Schema file name", "The name of the schema created for this connector", "sap-icon://form", null, true);
		properties.addIntegerProperty(POLLINTERVAL, "Poll interval [s]", "Every n seconds scan the directory for new files", "sap-icon://future", 20, true);
		properties.addStringProperty(TARGETTOPIC, "Topic name", "The topic name this producer should post all data", "sap-icon://cargo-train", "CSV_ROWS", true);
	}

	public FileProducerProperties(File dir, String name) throws PropertiesException {
		super(dir, name);
	}

	public String getSchemaFile() throws PropertiesException {
		return properties.getStringPropertyValue(FILESCHEMA);
	}
	
	public int getPollInterval() throws PropertiesException {
		return properties.getIntPropertyValue(POLLINTERVAL);
	}

	public String getTargetTopic() throws PropertiesException {
		return properties.getStringPropertyValue(TARGETTOPIC);
	}
}
