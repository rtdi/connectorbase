package io.rtdi.bigdata.connector.properties.atomic;

import java.util.List;

import io.rtdi.bigdata.connector.pipeline.foundation.entity.KeyValue;

public class PropertyStringSelector extends PropertyString {

	private List<KeyValue> options;

	public PropertyStringSelector() {
		super();
	}
	
	public PropertyStringSelector(String name, String displayname, String description, String icon, String defaultvalue, Boolean mandatory, List<KeyValue> options) {
		super(name, displayname, description, icon, defaultvalue, mandatory);
		this.options = options;
	}
	
	public void parseValue(IProperty property, boolean ignorepasswords) {
		if (property instanceof PropertyString) {
			this.value = ((PropertyString) property).getValue();
		} else {
			throw new Error("PropertyStringSelector value not of type PropertyStringSelector");
		}
	}

	@Override
	public IProperty clone(boolean ignorepasswords) {
		PropertyStringSelector c = new PropertyStringSelector(
				this.getName(),
				this.getDisplayname(),
				this.getDescription(),
				this.getIcon(),
				this.getValue(),
				this.getMandatory(),
				this.getOptions());
		return c;
	}

	public List<KeyValue> getOptions() {
		return options;
	}
	
}
