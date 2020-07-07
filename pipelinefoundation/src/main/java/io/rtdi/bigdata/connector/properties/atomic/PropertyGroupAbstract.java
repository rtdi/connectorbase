package io.rtdi.bigdata.connector.properties.atomic;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public abstract class PropertyGroupAbstract {

	protected List<IProperty> propertylist = new ArrayList<>();
	protected Hashtable<String, IProperty> nameindex = new Hashtable<>();
	protected boolean valuesset = false;
	protected String name;

	public PropertyGroupAbstract() {
		this("default");
	}
	
	public PropertyGroupAbstract(String name) {
		super();
		this.name = name;
	}
		
	public List<IProperty> getValues() {
		return propertylist;
	}
	
	/**
	 * This method is used for JAXB serialization only. To set all values of a property group setValue() based on a JAXB serialized property group use setValue(pg)
	 * 
	 * @param values list is the new topic property list
	 */
	public void setValues(List<IProperty> values) {
		propertylist = values;
	}
		
	public IProperty addProperty(IProperty prop) {
		propertylist.add(prop);
		nameindex.put(prop.getName(), prop);
		return prop;
	}
	
	
	public void setProperty(String name, String value) throws PropertiesException {
		IProperty element = getElement(name);
		if (element instanceof PropertyString) {
			((PropertyString)element).setValue(value);
		} else if (element instanceof PropertyPassword) {
			((PropertyPassword)element).setValue(value);
		} else if (element instanceof PropertyText) {
			((PropertyText)element).setValue(value);
		} else {
			throw new PropertiesException("A property of the name \"" + name + "\" exists but is not of a string type");
		}
	}

	public void setProperty(String name, Boolean value) throws PropertiesException {
		IProperty element = getElement(name);
		if (element instanceof PropertyBoolean) {
			((PropertyBoolean)element).setValue(value);
		} else {
			throw new PropertiesException("A property of the name \"" + name + "\" exists but is not of a boolean type");
		}
	}

	public void setProperty(String name, List<String> value) throws PropertiesException {
		IProperty element = getElement(name);
		if (element instanceof PropertyArrayList) {
			((PropertyArrayList)element).setValue(value);
		} else {
			throw new PropertiesException("A property of the name \"" + name + "\" exists but is not of an Array type");
		}
	}
	
	public void setProperty(String name, Integer value) throws PropertiesException {
		IProperty element = getElement(name);
		if (element instanceof PropertyInt) {
			((PropertyInt)element).setValue(value);
		} else {
			throw new PropertiesException("A property of the name \"" + name + "\" exists but is not of an Integer type");
		}
	}
	
	public void setProperty(String name, Long value) throws PropertiesException {
		IProperty element = getElement(name);
		if (element instanceof PropertyLong) {
			((PropertyLong)element).setValue(value);
		} else {
			throw new PropertiesException("A property of the name \"" + name + "\" exists but is not of a Long type");
		}
	}
	
	public IProperty getElement(String name) {
		IProperty p = nameindex.get(name);
		if (p == null) {
			// throw new PropertiesException("Propertygroup \"" + name + "\" does not have an element with name \"" + name + "\"");
			return null;
		} else {
			return p;
		}
	}

	public String getStringPropertyValue(String name) {
		IProperty e = getElement(name);
		if (e instanceof PropertyString) {
			return ((PropertyString) e).getValue();
		} else {
			// throw new PropertiesException("A property of the name \"" + name + "\" exists but is not of a string type");
			return null;
		}
	}

	public List<String> getMultiSchemaSelectorValue(String name) throws PropertiesException {
		IProperty e = getElement(name);
		if (e instanceof PropertyMultiSchemaSelector) {
			return ((PropertyMultiSchemaSelector) e).getValue();
		} else {
			throw new PropertiesException("A property of the name \"" + name + "\" exists but is not of a MultiSchemaSelector type");
		}
	}

	public Boolean getBooleanPropertyValue(String name) {
		IProperty e = getElement(name);
		if (e instanceof PropertyBoolean) {
			return ((PropertyBoolean) e).getValue();
		} else {
			// throw new PropertiesException("A property of the name \"" + name + "\" exists but is not of a boolean type");
			return null;
		}
	}

	public String getPasswordPropertyValue(String name) {
		IProperty e = getElement(name);
		if (e instanceof PropertyPassword) {
			return ((PropertyPassword) e).getValue();
		} else {
			// throw new PropertiesException("A property of the name \"" + name + "\" exists but is not of a Password type");
			return null;
		}
	}
	
	public List<String> getArrayListPropertyValue(String name) {
		IProperty e = getElement(name);
		if (e instanceof PropertyArrayList) {
			return ((PropertyArrayList) e).getValue();
		} else {
			// throw new PropertiesException("A property of the name \"" + name + "\" exists but is not of an Array type");
			return null;
		}
	}

	public Integer getIntPropertyValue(String name) {
		IProperty e = getElement(name);
		if (e instanceof PropertyInt) {
			return ((PropertyInt) e).getValue();
		} else {
			// throw new PropertiesException("A property of the name \"" + name + "\" exists but is not of an Integer type");
			return null;
		}
	}

	public Long getLongPropertyValue(String name) {
		IProperty e = getElement(name);
		if (e instanceof PropertyLong) {
			return ((PropertyLong) e).getValue();
		} else {
			// throw new PropertiesException("A property of the name \"" + name + "\" exists but is not of a Long type");
			return null;
		}
	}

	public PropertyGroup getPropertyGroup(String name) {
		IProperty e = getElement(name);
		if (e instanceof PropertyGroup) {
			return ((PropertyGroup) e);
		} else {
			// throw new PropertiesException("A property of the name \"" + name + "\" exists but is not a PropertyGroup");
			return null;
		}
	}

	
	public void addPasswordProperty(String name, String displayname, String description, String icon, String defaultvalue, boolean mandatory) {
		addProperty(new PropertyPassword(name, displayname, description, icon, defaultvalue, mandatory));
	}

	public void addIntegerProperty(String name, String displayname, String description, String icon, Integer defaultvalue, boolean mandatory) {
		addProperty(new PropertyInt(name, displayname, description, icon, defaultvalue, mandatory));
	}

	public void addLongProperty(String name, String displayname, String description, String icon, Long defaultvalue, boolean mandatory) {
		addProperty(new PropertyLong(name, displayname, description, icon, defaultvalue, mandatory));
	}

	public void addStringProperty(String name, String displayname, String description, String icon, String defaultvalue, boolean mandatory) {
		addProperty(new PropertyString(name, displayname, description, icon, defaultvalue, mandatory));
	}

	public void addSchemaSelector(String name, String displayname, String description, String icon, String defaultvalue, boolean mandatory) {
		addProperty(new PropertySchemaSelector(name, displayname, description, icon, defaultvalue, mandatory));
	}

	public void addTopicSelector(String name, String displayname, String description, String icon, String defaultvalue, boolean mandatory) {
		addProperty(new PropertyTopicSelector(name, displayname, description, icon, defaultvalue, mandatory));
	}

	public void addMultiSchemaSelectorProperty(String name, String displayname, String description, String icon, boolean mandatory) {
		addProperty(new PropertyMultiSchemaSelector(name, displayname, description, icon, mandatory));
	}

	public void addTextProperty(String name, String displayname, String description, String icon, String defaultvalue, boolean mandatory) {
		addProperty(new PropertyText(name, displayname, description, icon, defaultvalue, mandatory));
	}

	public void addBooleanProperty(String name, String displayname, String description, String icon, Boolean defaultvalue, boolean mandatory) {
		addProperty(new PropertyBoolean(name, displayname, description, icon, defaultvalue, mandatory));
	}

	public void addArrayListProperty(String name, String displayname, String description, String icon, boolean mandatory) {
		addProperty(new PropertyArrayList(name, displayname, description, icon, mandatory));
	}
	
	public PropertyGroup addPropertyGroupProperty(String name, String displayname, String description, String icon, boolean mandatory) {
		PropertyGroup p = new PropertyGroup(name, displayname, description, icon, mandatory);
		addProperty(p);
		return p;
	}
	
	@JsonIgnore
	public boolean isValueSet() {
		return valuesset;
	}

	@Override
	public String toString() {
		return String.valueOf(propertylist);
	}

	public void parseValue(PropertyGroupAbstract value, boolean ignorepasswords) throws PropertiesException {
		this.valuesset = false;
		PropertyGroupAbstract pg = (PropertyGroupAbstract) value;
		for (IProperty v : pg.getValues()) {
			if (v.getName() == null) {
				throw new PropertiesException("The passed property element does not have a name", "Should actually be impossible", 10006, v.toString());
			}
			IProperty e = nameindex.get(v.getName());
			if (e == null) {
				IProperty n = v.clone(ignorepasswords);
				nameindex.put(n.getName(), n);
				propertylist.add(n);
			} else {
				if (e instanceof PropertyGroup) {
					((PropertyGroup) e).parseValue((PropertyGroup) v, ignorepasswords);
				} else if (e instanceof IPropertyValue) {
					((IPropertyValue) e).parseValue(v, ignorepasswords);						
				}
			}
		}
		
		// Check all mandatory parameters are set
		for (IProperty p : nameindex.values()) {
			if (p instanceof IPropertyValue) {
				IPropertyValue valueproperty = (IPropertyValue) p;
				if (valueproperty.getMandatory() != null && valueproperty.getMandatory().booleanValue() && !valueproperty.hasValue()) {
					// Cannot throw exception here, what if nothing has been set yet?
					// throw new PropertiesException("A mandatory parameter in the properties is not set", "Fix the properties settings", 10007, valueproperty.getName());
				}
			}
		}
		this.valuesset = true;
	}

}
