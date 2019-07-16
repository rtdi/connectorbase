package io.rtdi.bigdata.connector.pipeline.foundation;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

/**
 * The schema name is a string concat in the form of &lt;<i>tenantid</i>&gt;-&lt;<i>schemaname</i>&gt;.
 * This class makes sure all names are of the proper format and helps to access
 * the FQN, the name (without tenant) or the tenant portion.
 *
 */
public class SchemaName implements Comparable<SchemaName>{
	private String fqn;
	private String tenant;
	private String name;

	/**
	 * Based on a FQN creates the name parts. Normally the string passed in is the format of 
	 * &lt;<i>tenantid</i>&gt;-&lt;<i>schemaname</i>&gt; but simple strings (without tenant) are allowed as well.
	 * Then the tenant is null.
	 * 
	 * @param fqn is the full qualified name of the topic
	 * @throws PropertiesException in case the fqn is null or invalid
	 */
	public SchemaName(String fqn) throws PropertiesException {
		if (fqn == null) {
			throw new PropertiesException("Schemaname cannot be constructed from an empty string");
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
	 * Build the schema name based on the tenant and name.
	 * 
	 * @param tenantID can be null
	 * @param schemaname is the tenant specific name of the schema
	 * @throws PropertiesException in case the fqn is null
	 */
	public SchemaName(String tenantID, String schemaname) throws PropertiesException {
		if (schemaname == null || schemaname.length() == 0) {
			throw new PropertiesException("schemaname cannot be null or empty");
		}
		StringBuffer b = new StringBuffer();
		if (tenantID != null) {
			b.append(TopicUtil.validate(tenantID));
			b.append("-");
		}
		b.append(TopicUtil.validate(schemaname));
		this.fqn = b.toString();
		this.tenant = tenantID;
		this.name = schemaname;
	}

	@Override
	public int hashCode() {
		return fqn.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		} else if (obj instanceof SchemaName) {
			SchemaName s = (SchemaName) obj;
			return fqn.equals(s.fqn);
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return fqn;
	}

	/**
	 * @return the schema fully qualified name
	 */
	public String getSchemaFQN() {
		return fqn;
	}

	/**
	 * Duplicate the string operations based on the fqn.
	 * 
	 * @param c character to find
	 * @return fqn.indexOf(c)
	 */
	public int indexOf(char c) {
		return fqn.indexOf(c);
	}

	/**
	 * Duplicate the string operations based on the fqn.
	 * 
	 * @param beginindex start position
	 * @param len length of the substring
	 * @return fqn.substring(beginindex, len)
	 */
	public String substring(int beginindex, int len) {
		return fqn.substring(beginindex, len);
	}

	/**
	 * Duplicate the string operations based on the fqn.
	 * 
	 * @param beginindex start position
	 * @return fqn.substring(beginindex)
	 */
	public String substring(int beginindex) {
		return fqn.substring(beginindex);
	}
	

	/**
	 * @return The TenantID of the schema
	 */
	public String getTenant() {
		return tenant;
	}

	/**
	 * @return The tenant specific name part of the schema
	 */
	public String getName() {
		return name;
	}

	@Override
	public int compareTo(SchemaName o) {
		if (o == null) {
			return -1;
		} else {
			return fqn.compareTo(o.fqn);
		}
	}

}
