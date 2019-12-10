package io.rtdi.bigdata.connector.properties.atomic;

import java.util.List;

public class PropertyArrayList extends PropertyAbstract implements IPropertyValue {
	protected List<String> value;

	public PropertyArrayList() {
		super();
	}
	
	public PropertyArrayList(String name, String displayname, String description, String icon, Boolean mandatory) {
		super(name, displayname, description, icon, mandatory);
	}
	
	public void setValue(List<String> value) {
		this.value = value;
	}
	
	@Override
	public List<String> getValue() {
		return value;
	}
	
	public void parseValue(IProperty property, boolean ignorepasswords) {
		if (property instanceof PropertyArrayList) {
			this.value = ((PropertyArrayList) property).getValue();
		} else {
			throw new Error("PropertyArrayList's value not of type PropertyArrayList");
		}
	}

	@Override
	public boolean hasValue() {
		return value != null && value.size() != 0;
	}

	@Override
	public IProperty clone(boolean ignorepasswords) {
		PropertyArrayList c = new PropertyArrayList(this.getName(), this.getDisplayname(), this.getDescription(), this.getIcon(), this.getMandatory());
		// TODO: Copy values instead of replacing the array
		c.setValue(getValue());
		return c;
	}

}
