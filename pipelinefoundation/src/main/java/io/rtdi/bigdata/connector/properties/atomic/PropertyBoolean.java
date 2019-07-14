package io.rtdi.bigdata.connector.properties.atomic;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class PropertyBoolean extends PropertyAbstract implements IPropertyValue {
	protected Boolean value;

	public PropertyBoolean() {
		super();
	}
	
	public PropertyBoolean(String name, Boolean value) {
		this(name, null, null, null, value, null);
	}
	
	public PropertyBoolean(String name, String displayname, String description, String icon, Boolean defaultvalue, Boolean mandatory) {
		super(name, displayname, description, icon, mandatory);
		setValue(defaultvalue);
	}

	public void setValue(Boolean value) {
		this.value = value;
	}

	@Override
	public Boolean getValue() {
		return value;
	}

	@Override
	public void parseValue(IProperty property) throws PropertiesException {
		if (property instanceof PropertyBoolean) {
			this.value = ((PropertyBoolean) property).getValue();
		} else {
			throw new PropertiesException("PropertyBoolean's value not of type PropertyBoolean");
		}
	}

	@Override
	public String toString() {
		return String.valueOf(value);
	}

}
