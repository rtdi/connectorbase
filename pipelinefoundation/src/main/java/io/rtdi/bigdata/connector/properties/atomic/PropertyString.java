package io.rtdi.bigdata.connector.properties.atomic;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class PropertyString extends PropertyAbstract implements IPropertyValue {
	protected String value;

	public PropertyString() {
		super();
	}
	
	public PropertyString(String name, String value) {
		this(name, null, null, null, value, null);
	}
	
	public PropertyString(String name, String displayname, String description, String icon, String defaultvalue, Boolean mandatory) {
		super(name, displayname, description, icon, mandatory);
		setValue(defaultvalue);
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public String getValue() {
		return value;
	}

	@Override
	public void parseValue(IProperty property) throws PropertiesException {
		if (property instanceof PropertyString) {
			this.value = ((PropertyString) property).getValue();
		} else {
			throw new PropertiesException("PropertyString's value not of type PropertyString");
		}
	}

	@Override
	public String toString() {
		return value;
	}

	@Override
	public boolean hasValue() {
		return value != null && value.length() != 0;
	}

}
