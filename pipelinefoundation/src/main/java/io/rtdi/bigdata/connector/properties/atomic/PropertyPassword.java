package io.rtdi.bigdata.connector.properties.atomic;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class PropertyPassword extends PropertyAbstract implements IPropertyValue {
	protected String value;

	public PropertyPassword() {
		super();
	}
	
	public PropertyPassword(String name, String value) {
		this(name, null, null, null, value, null);
	}

	public PropertyPassword(String name, String displayname, String description, String icon, String defaultvalue, Boolean mandatory) {
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

	@JsonIgnore
	public String getPassword() {
		return value;
	}

	@Override
	public String toString() {
		return "";
	}

	@Override
	public void parseValue(IProperty property, boolean ignorepasswords) throws PropertiesException {
		if (property instanceof PropertyPassword) {
			if (ignorepasswords) {
				this.value = null;
			} else {
				this.value = ((PropertyPassword) property).getValue();
			}
		} else {
			throw new PropertiesException("PropertyPassword's value not of type PropertyPassword");
		}
	}

	@Override
	public boolean hasValue() {
		return value != null;
	}

	@Override
	public IProperty clone(boolean ignorepasswords) {
		return new PropertyPassword(this.getName(), this.getDisplayname(), this.getDescription(), this.getIcon(), (ignorepasswords==false?this.getValue():"******"), this.getMandatory());
	}

}
