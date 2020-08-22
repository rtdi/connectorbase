package io.rtdi.bigdata.connector.pipeline.foundation;

import java.time.Duration;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.SchemaNameEncoder;

/**
 * The schema name is a string that has an encoded equivalent to be valid for the schema registry.
 * Note that the schema name is actually the subject name part used for schema registry (which adds the text -key or -value)
 *
 */
public class SchemaRegistryName implements Comparable<SchemaRegistryName>{
	private String name;
	private String encodedname;
	private static Cache<String, SchemaRegistryName> schemanamecache = Caffeine.newBuilder().expireAfterAccess(Duration.ofMinutes(30)).maximumSize(10000).build();


	/**
	 * Build the schema name based on the tenant and name.
	 * 
	 * @param schemaname is the tenant specific name of the schema
	 * @throws PropertiesException in case the fqn is null
	 */
	private SchemaRegistryName(String schemaname) throws PropertiesException {
		if (schemaname == null || schemaname.length() == 0) {
			throw new PropertiesException("schemaname cannot be null or empty");
		}
		this.name = schemaname;
		this.encodedname = SchemaNameEncoder.encodeName(name);
	}

	/**
	 * Factory method to create a new SchemaName
	 * @param name is an arbitrary name, even names that are not supported by Kafka are fine 
	 * @return a new SchemaName object
	 */
	public static SchemaRegistryName create(String name) {
		return schemanamecache.get(name, k -> getNewSchemaName(k));
	}
	
	private static SchemaRegistryName getNewSchemaName(String name) {
		try {
			return new SchemaRegistryName(name);
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
		} else if (obj instanceof SchemaRegistryName) {
			SchemaRegistryName s = (SchemaRegistryName) obj;
			return name.equals(s.name);
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return name;
	}

	public String getEncodedName() {
		return encodedname;
	}

	/**
	 * Duplicate the string operations based on the name.
	 * 
	 * @param c character to find
	 * @return fqn.indexOf(c)
	 */
	public int indexOf(char c) {
		return name.indexOf(c);
	}

	/**
	 * Duplicate the string operations based on the name.
	 * 
	 * @param beginindex start position
	 * @param len length of the substring
	 * @return fqn.substring(beginindex, len)
	 */
	public String substring(int beginindex, int len) {
		return name.substring(beginindex, len);
	}

	/**
	 * Duplicate the string operations based on the name.
	 * 
	 * @param beginindex start position
	 * @return fqn.substring(beginindex)
	 */
	public String substring(int beginindex) {
		return name.substring(beginindex);
	}
	

	/**
	 * @return The name of the schema
	 */
	public String getName() {
		return name;
	}

	@Override
	public int compareTo(SchemaRegistryName o) {
		if (o == null) {
			return -1;
		} else {
			return name.compareTo(o.name);
		}
	}

}
