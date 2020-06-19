package io.rtdi.bigdata.connector.properties.atomic;

public class PropertyTopicSelector extends PropertyString {

	public PropertyTopicSelector() {
		super();
	}
	
	public PropertyTopicSelector(String name, String displayname, String description, String icon, String defaultvalue, Boolean mandatory) {
		super(name, displayname, description, icon, defaultvalue, mandatory);
	}
	
	public void parseValue(IProperty property, boolean ignorepasswords) {
		if (property instanceof PropertyString) {
			this.value = ((PropertyString) property).getValue();
		} else {
			throw new Error("PropertyTopicSelector value not of type PropertyTopicSelector");
		}
	}

	@Override
	public IProperty clone(boolean ignorepasswords) {
		PropertyTopicSelector c = new PropertyTopicSelector(this.getName(), this.getDisplayname(), this.getDescription(), this.getIcon(), this.getValue(), this.getMandatory());
		return c;
	}

}
