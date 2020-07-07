package io.rtdi.bigdata.connector.properties.atomic;

import java.util.ArrayList;
import java.util.List;

public class PropertyMultiSchemaSelector extends PropertyAbstract implements IPropertyValue {
	protected List<String> value;

	public PropertyMultiSchemaSelector() {
		super();
	}
	
	public PropertyMultiSchemaSelector(String name, String displayname, String description, String icon, Boolean mandatory) {
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
		if (property instanceof PropertyMultiSchemaSelector) {
			this.value = ((PropertyMultiSchemaSelector) property).getValue();
		} else {
			throw new Error("PropertySchemaSelector's value not of type PropertySchemaSelector");
		}
	}

	@Override
	public boolean hasValue() {
		return value != null && value.size() != 0;
	}

	@Override
	public IProperty clone(boolean ignorepasswords) {
		PropertyMultiSchemaSelector c = new PropertyMultiSchemaSelector(this.getName(), this.getDisplayname(), this.getDescription(), this.getIcon(), this.getMandatory());
		List<String> l = new ArrayList<String>();
		if (value != null) {
			l.addAll(value);
		}
		c.setValue(l);
		return c;
	}

}
