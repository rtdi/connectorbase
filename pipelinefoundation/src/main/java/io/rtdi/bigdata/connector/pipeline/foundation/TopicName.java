package io.rtdi.bigdata.connector.pipeline.foundation;

import java.time.Duration;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.TopicNameEncoder;

/**
 * Represents the name of a topic.
 *
 */
public class TopicName implements Comparable<TopicName> {
	private String name;
	private String encodedname;
	
	private static Cache<String, TopicName> topicnamecache = Caffeine.newBuilder().expireAfterAccess(Duration.ofMinutes(30)).maximumSize(10000).build();

	/**
	 * Create a topic name based on the tenant and the tenant specific name. 
	 * 
	 * @param name topic name within the tenant
	 * @throws PropertiesException in case the name is null
	 */
	private TopicName(String name) throws PropertiesException {
		if (name == null || name.length() == 0) {
			throw new PropertiesException("Topicname cannot be null or empty");
		}
		this.name = name;
		this.encodedname = TopicNameEncoder.encodeName(name);
	}
	
	/**
	 * Factory method to create a new TopicName object
	 * @param name is an arbitrary text, even names not supported by Kafka are fine as it will be encoded
	 * @return the TopicName object
	 */
	public static TopicName create(String name) {
		return topicnamecache.get(name, k -> getNewTopicName(k));
	}

	/**
	 * Factory method to create a new TopicName object
	 * @param name is an actual Kafka topic name
	 * @return the TopicName object
	 */
	public static TopicName createViaEncoded(String name) {
		String realname = TopicNameEncoder.decodeName(name);
		return topicnamecache.get(realname, k -> getNewTopicName(k));
	}

	private static TopicName getNewTopicName(String name) {
		try {
			return new TopicName(name);
		} catch (PropertiesException e) {
			return null; // cannot happen actually
		}
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
	
	/**
	 * @return the encoded name, that is a name where illegal characters are replaced 
	 */
	public String getEncodedName() {
		return encodedname;
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
