package io.rtdi.bigdata.connector.properties.atomic;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;


public class PropertyLong extends PropertyAbstract implements IPropertyValue {
	protected Long value;

	public PropertyLong() {
		super();
	}
	
	public PropertyLong(String name, Long value) {
		this(name, null, null, null, value, null);
	}
	
	public PropertyLong(String name, String displayname, String description, String icon, Long defaultvalue, Boolean mandatory) {
		super(name, displayname, description, icon, mandatory);
		setValue(defaultvalue);
	}


	public void setValue(Long value) {
		this.value = value;
	}

	@Override
	public Long getValue() {
		return value;
	}

	@Override
	public void parseValue(IProperty property) throws PropertiesException {
		if (property instanceof PropertyLong) {
			this.value = ((PropertyLong) property).getValue();
		} else {
			throw new PropertiesException("PropertyLong's value not of type PropertyLong");
		}
	}

	@Override
	public String toString() {
		return String.valueOf(value);
	}

	@Override
	public boolean hasValue() {
		return value != null;
	}

}
