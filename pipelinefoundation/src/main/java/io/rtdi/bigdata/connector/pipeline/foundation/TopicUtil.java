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

	public static String extractName(String fqn) throws PropertiesException {
		if (fqn == null) {
			return null;
		} else {
			int pos = fqn.indexOf('-');
			if (pos == -1 || pos >= fqn.length()) {
				throw new PropertiesException("Not a valid <Tenant>-<Topicname> fqn (\"" + fqn + "\")");
			} else {
				return fqn.substring(pos+1);
			}
		}
	}
	
	
	/**
	 * Used to combine tenant and topic name without validation. Useful for example when the topic name is actually a regex pattern.
	 * @param tenantid
	 * @param name
	 * @return fqn
	 */
	public static String createTopicFQN(String tenantid, String name) throws PropertiesException {
		StringBuffer b = new StringBuffer();
		if (tenantid != null) {
			b.append(TopicUtil.validate(tenantid));
			b.append("-");
		}
		b.append(TopicUtil.validate(name));
		return b.toString();
	}
}
