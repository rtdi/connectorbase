package io.rtdi.bigdata.connector.properties.atomic;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public abstract class PropertyAbstract implements INamed {
	private String name;
	private String description;
	private String displayname;
	private String icon;
	private Boolean mandatory;
	private Boolean enabled;
	private String errormessage;
	private String erroricon;

	public PropertyAbstract() {
		this(null, null, null, null, null);
	}

	public PropertyAbstract(String name) {
		this(name, null, null, null, null);
	}

	public PropertyAbstract(String name, String displayname, String description) {
		this(name, displayname, description, null, null);
	}

	public PropertyAbstract(String name, String displayname, String description, String icon, Boolean mandatory) {
		super();
		this.name = name;
		this.description = description;
		this.displayname = displayname;
		this.icon = icon;
		this.mandatory = mandatory;
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.properties.atomic.INamed#getName()
	 */
	@Override
	public String getName() {
		return name;
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.properties.atomic.INamed#setName(java.lang.String)
	 */
	@Override
	public void setName(String name) {
		this.name = name;
	}

	@Override
	public int hashCode() {
		return name.hashCode();
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.properties.atomic.INamed#getDescription()
	 */
	@Override
	public String getDescription() {
		if (description == null) {
			return getDisplayname();
		} else {
			return description;
		}
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.properties.atomic.INamed#setDescription(java.lang.String)
	 */
	@Override
	public void setDescription(String description) {
		this.description = description;
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.properties.atomic.INamed#getDisplayname()
	 */
	@Override
	public String getDisplayname() {
		if (displayname == null) {
			return name;
		} else {
			return displayname;
		}
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.properties.atomic.INamed#setDisplayname(java.lang.String)
	 */
	@Override
	public void setDisplayname(String displayname) {
		this.displayname = displayname;
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.properties.atomic.INamed#getIcon()
	 */
	@Override
	public String getIcon() {
		return icon;
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.properties.atomic.INamed#setIcon(java.lang.String)
	 */
	@Override
	public void setIcon(String icon) {
		this.icon = icon;
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.properties.atomic.INamed#getMandatory()
	 */
	@Override
	public Boolean getMandatory() {
		return mandatory;
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.properties.atomic.INamed#setMandatory(java.lang.Boolean)
	 */
	@Override
	public void setMandatory(Boolean mandatory) {
		this.mandatory = mandatory;
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.properties.atomic.INamed#getEnabled()
	 */
	@Override
	public Boolean getEnabled() {
		return enabled;
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.properties.atomic.INamed#setEnabled(java.lang.Boolean)
	 */
	@Override
	public void setEnabled(Boolean enabled) {
		this.enabled = enabled;
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.properties.atomic.INamed#getErrormessage()
	 */
	@Override
	public String getErrormessage() {
		return errormessage;
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.properties.atomic.INamed#setErrormessage(java.lang.String)
	 */
	@Override
	public void setErrormessage(String errormessage) {
		this.errormessage = errormessage;
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.properties.atomic.INamed#getErroricon()
	 */
	@Override
	public String getErroricon() {
		return erroricon;
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.properties.atomic.INamed#setErroricon(java.lang.String)
	 */
	@Override
	public void setErroricon(String erroricon) {
		this.erroricon = erroricon;
	}

}
