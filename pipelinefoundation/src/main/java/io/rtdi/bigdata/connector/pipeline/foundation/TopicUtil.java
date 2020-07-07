package io.rtdi.bigdata.connector.pipeline.foundation;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

/**
 * Some helper methods around topics
 *
 */
public class TopicUtil {

	public static String validate(String s) throws PropertiesException {
		if (s == null) {
			return s;
		} else if (s.contains("-")) {
			throw new PropertiesException("A - char is not allowed in the topic name parts (" + s + ")");
//		} else if (s.contains(".")) {
//			throw new PropertiesException("A . char is not allowed in the topic name parts (" + s + ")");
		} else {
			return s;
		}
	}

}
