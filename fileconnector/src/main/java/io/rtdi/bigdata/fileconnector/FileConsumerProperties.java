package io.rtdi.bigdata.fileconnector;

import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;

public class FileConsumerProperties extends ConsumerProperties {

	public FileConsumerProperties(String name) throws PropertiesException {
		super(name);
		// TODO Auto-generated constructor stub
	}

	public FileConsumerProperties(String name, TopicName topic) throws PropertiesException {
		super(name, topic);
		// TODO Auto-generated constructor stub
	}

	public FileConsumerProperties(String name, String pattern) throws PropertiesException {
		super(name, pattern);
		// TODO Auto-generated constructor stub
	}

}
