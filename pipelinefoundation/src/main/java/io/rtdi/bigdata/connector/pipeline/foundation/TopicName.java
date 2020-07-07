package io.rtdi.bigdata.connector.pipeline.foundation;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

/**
 * Represents the name of a topic to simplify tenant, name and fqn name handling.
 *
 */
public class TopicName implements Comparable<TopicName> {
	private String name;

	/**
	 * Create a topic name based on the tenant and the tenant specific name. 
	 * 
	 * @param name topic name within the tenant
	 * @throws PropertiesException in case the name is null
	 */
	public TopicName(String name) throws PropertiesException {
		if (name == null || name.length() == 0) {
			throw new PropertiesException("Topicname cannot be null or empty");
		}
		this.name = name;
	}
	
	@Override
	public int hashCode() {
		return name.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		} else if (obj instanceof TopicName) {
			TopicName t = (TopicName) obj;
			return name.equals(t.name);
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return name;
	}

	/**
	 * @param c character to find
	 * @return fqn.indexOf(c)
	 */
	public int indexOf(char c) {
		return name.indexOf(c);
	}

	/**
	 * @param beginindex start position
	 * @param len substring length
	 * @return name.substring(beginindex, len)
	 */
	public String substring(int beginindex, int len) {
		return name.substring(beginindex, len);
	}

	/**
	 * @param beginindex start position
	 * @return name.substring(beginindex)
	 */
	public String substring(int beginindex) {
		return name.substring(beginindex);
	}

	/**
	 * @return The topic name
	 */
	public String getName() {
		return name;
	}
	
	@Override
	public int compareTo(TopicName o) {
		if (o == null) {
			return -1;
		} else {
			return name.compareTo(o.name);
		}
	}

}
