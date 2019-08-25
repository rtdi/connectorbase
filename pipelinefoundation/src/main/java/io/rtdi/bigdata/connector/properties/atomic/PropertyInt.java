package io.rtdi.bigdata.connector.properties.atomic;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class PropertyInt extends PropertyAbstract implements IPropertyValue {
	protected Integer value;

	public PropertyInt() {
		super();
	}
	
	public PropertyInt(String name, Integer value) {
		this(name, null, null, null, value, null);
	}

	public PropertyInt(String name, String displayname, String description, String icon, Integer defaultvalue, Boolean mandatory) {
		super(name, displayname, description, icon, mandatory);
		setValue(defaultvalue);
	}

	public void setValue(Integer value) {
		this.value = value;
	}

	@Override
	public Integer getValue() {
		return value;
	}

	@Override
	public void parseValue(IProperty property, boolean ignorepasswords) throws PropertiesException {
		if (property instanceof PropertyInt) {
			this.value = ((PropertyInt) property).getValue();
		} else {
			throw new PropertiesException("PropertyInt's value not of type PropertyInt");
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

	@Override
	public IProperty clone(boolean ignorepasswords) {
		return new PropertyInt(this.getName(), this.getDisplayname(), this.getDescription(), this.getIcon(), this.getValue(), this.getMandatory());
	}

}
