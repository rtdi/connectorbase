package io.rtdi.bigdata.connector.properties.atomic;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class PropertyGroup extends PropertyGroupAbstract implements IProperty, INamed {
	private String description;
	private String displayname;
	private String icon;
	private Boolean mandatory;
	private Boolean enabled;
	private String errormessage;
	private String erroricon;

	public PropertyGroup() {
		this("default");
	}
	
	public PropertyGroup(String name) {
		super(name);
	}
		
	public PropertyGroup(String name, String displayname, String description) {
		this(name, displayname, description, null, null);
	}

	public PropertyGroup(String name, String displayname, String description, String icon, Boolean mandatory) {
		super(name);
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
			return name;
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

	public void parseValue(IProperty value) throws PropertiesException {
		if (value instanceof PropertyGroupAbstract) {
			PropertyGroupAbstract pg = (PropertyGroupAbstract) value;
			for (IProperty v : pg.getValues()) {
				if (v.getName() == null) {
					throw new PropertiesException("The passed element \"" + v.toString() + "\" does not have a name");
				}
				IProperty e = nameindex.get(v.getName());
				if (e == null) {
					nameindex.put(v.getName(), v);
					propertylist.add(v);
				} else {
					if (e instanceof PropertyGroup) {
						((PropertyGroup) e).parseValue(v);
					} else if (e instanceof IPropertyValue) {
						((IPropertyValue) e).parseValue(v);						
					}
				}
			}
			this.valuesset = true;
		} else {
			throw new PropertiesException("PropertyGroup value not of type PropertyGroup");
		}
	}

}
