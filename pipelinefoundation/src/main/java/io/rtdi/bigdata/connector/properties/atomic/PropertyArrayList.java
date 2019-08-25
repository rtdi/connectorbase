package io.rtdi.bigdata.connector.properties.atomic;

import java.util.ArrayList;

public class PropertyArrayList extends PropertyAbstract implements IPropertyValue {
	protected ArrayList<PropertyString> value;

	public PropertyArrayList() {
		super();
	}
	
	public PropertyArrayList(String name, ArrayList<String> value1, ArrayList<PropertyString> value2) {
		this(name, null, null, null, value1, value2, null);
	}

	// TODO: Copy values instead of replacing the array
	public PropertyArrayList(String name, String displayname, String description, String icon, ArrayList<String> value1, ArrayList<PropertyString> value2, Boolean mandatory) {
		super(name, displayname, description, icon, mandatory);
		if (value1 != null) {
			setStringValue(value1);
		} else {
			value = value2;
		}
	}

	public PropertyArrayList(String name, String displayname, String description, String icon, Boolean mandatory) {
		super(name, displayname, description, icon, mandatory);
	}
	
	public void setValue(ArrayList<PropertyString> value) {
		this.value = value;
	}
	
	public void setStringValue(ArrayList<String> value) {
		if (value != null) {
			this.value = new ArrayList<PropertyString>();
			for (String s : value) {
				this.value.add(new PropertyString("value", s));
			}
		}
	}
	
	@Override
	public ArrayList<PropertyString> getValue() {
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
