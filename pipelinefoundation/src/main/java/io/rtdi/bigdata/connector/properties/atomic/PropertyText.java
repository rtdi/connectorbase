package io.rtdi.bigdata.connector.properties.atomic;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class PropertyText extends PropertyAbstract implements IPropertyValue {
	protected String value;

	public PropertyText() {
		super();
	}
	
	public PropertyText(String name, String value) {
		this(name, null, null, null, value, null);
	}
	
	public PropertyText(String name, String displayname, String description, String icon, String defaultvalue, Boolean mandatory) {
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
	public void parseValue(IProperty property, boolean ignorepasswords) throws PropertiesException {
		if (property instanceof PropertyText) {
			this.value = ((PropertyText) property).getValue();
		} else {
			throw new PropertiesException("PropertyText's value not of type PropertyText");
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

	@Override
	public IProperty clone(boolean ignorepasswords) {
		return new PropertyText(this.getName(), this.getDisplayname(), this.getDescription(), this.getIcon(), this.getValue(), this.getMandatory());
	}

}
