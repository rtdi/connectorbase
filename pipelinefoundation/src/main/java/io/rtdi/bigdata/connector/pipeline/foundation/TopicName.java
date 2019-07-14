package io.rtdi.bigdata.connector.pipeline.foundation;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

/**
 * Represents the name of a topic to simplify tenant, name and fqn name handling.
 *
 */
public class TopicName implements Comparable<TopicName> {
	private String fqn;
	private String tenant;
	private String name;

	/**
	 * Create a topic name based on the FQN.
	 * 
	 * @param fqn
	 * @throws PropertiesException
	 */
	public TopicName(String fqn) throws PropertiesException {
		if (fqn == null) {
			throw new PropertiesException("Topic cannot be constructed from an empty string");
		}
		int tenantsplitpos = fqn.indexOf('-');
		if (tenantsplitpos != -1) {
			tenant = TopicUtil.validate(fqn.substring(0, tenantsplitpos));
			name = TopicUtil.validate(fqn.substring(tenantsplitpos+1));
		} else {
			tenant = null;
			name = fqn.toString();
		}
		this.fqn = fqn;
	}

	/**
	 * Create a topic name based on the tenant and the tenant specific name. 
	 * 
	 * @param tenantID optional
	 * @param name
	 * @throws PropertiesException
	 */
	public TopicName(String tenantID, String name) throws PropertiesException {
		if (name == null || name.length() == 0) {
			throw new PropertiesException("Topicname cannot be null or empty");
		}
		this.fqn = TopicUtil.createTopicFQN(tenantID, name);
		this.name = name;
		this.tenant = tenantID;
	}
	
	@Override
	public int hashCode() {
		return fqn.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		} else if (obj instanceof TopicName) {
			TopicName t = (TopicName) obj;
			return fqn.equals(t.fqn);
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return fqn;
	}

	/**
	 * @return the fully qualified topic name string
	 */
	public String getTopicFQN() {
		return fqn;
	}

	/**
	 * @param c
	 * @return fqn.indexOf(c)
	 */
	public int indexOf(char c) {
		return fqn.indexOf(c);
	}

	/**
	 * @param beginindex
	 * @param len
	 * @return fqn.substring(beginindex, len)
	 */
	public String substring(int beginindex, int len) {
		return fqn.substring(beginindex, len);
	}

	/**
	 * @param beginindex
	 * @return fqn.substring(beginindex)
	 */
	public String substring(int beginindex) {
		return fqn.substring(beginindex);
	}

	/**
	 * @return the tenant part of the topic name
	 */
	public String getTenant() {
		return tenant;
	}

	/**
	 * @return The tenant specific topic name
	 */
	public String getName() {
		return name;
	}
	
	@Override
	public int compareTo(TopicName o) {
		if (o == null) {
			return -1;
		} else {
			return fqn.compareTo(o.fqn);
		}
	}

}
