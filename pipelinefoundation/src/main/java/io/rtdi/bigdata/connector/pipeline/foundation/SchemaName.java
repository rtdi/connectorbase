package io.rtdi.bigdata.connector.pipeline.foundation;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

/**
 * The schema name is a string concat in the form of &lt;<i>tenantid</i>&gt;-&lt;<i>schemaname</i>&gt;.
 * This class makes sure all names are of the proper format and helps to access
 * the FQN, the name (without tenant) or the tenant portion.
 *
 */
public class SchemaName implements Comparable<SchemaName>{
	private String name;

	/**
	 * Build the schema name based on the tenant and name.
	 * 
	 * @param schemaname is the tenant specific name of the schema
	 * @throws PropertiesException in case the fqn is null
	 */
	public SchemaName(String schemaname) throws PropertiesException {
		if (schemaname == null || schemaname.length() == 0) {
			throw new PropertiesException("schemaname cannot be null or empty");
		}
		TopicUtil.validate(schemaname);
		this.name = schemaname;
	}

	@Override
	public int hashCode() {
		return name.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		} else if (obj instanceof SchemaName) {
			SchemaName s = (SchemaName) obj;
			return name.equals(s.name);
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return name;
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
	public int compareTo(SchemaName o) {
		if (o == null) {
			return -1;
		} else {
			return name.compareTo(o.name);
		}
	}

}
