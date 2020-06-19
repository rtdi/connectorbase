package io.rtdi.bigdata.connector.properties.atomic;

public class PropertySchemaSelector extends PropertyString {

	public PropertySchemaSelector() {
		super();
	}
	
	public PropertySchemaSelector(String name, String displayname, String description, String icon, String defaultvalue, Boolean mandatory) {
		super(name, displayname, description, icon, defaultvalue, mandatory);
	}
	
	public void parseValue(IProperty property, boolean ignorepasswords) {
		if (property instanceof PropertyString) {
			this.value = ((PropertyString) property).getValue();
		} else {
			throw new Error("PropertySchemaSelector's value not of type PropertySchemaSelector");
		}
	}

	@Override
	public IProperty clone(boolean ignorepasswords) {
		PropertySchemaSelector c = new PropertySchemaSelector(this.getName(), this.getDisplayname(), this.getDescription(), this.getIcon(), this.getValue(), this.getMandatory());
		return c;
	}

}
