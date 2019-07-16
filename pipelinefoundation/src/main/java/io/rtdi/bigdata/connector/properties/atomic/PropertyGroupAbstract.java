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

	public void setProperty(String name, ArrayList<PropertyString> value) throws PropertiesException {
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
	
	public ArrayList<PropertyString> getArrayListPropertyValue(String name) {
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

	
	public void addPasswordProperty(String name, String displayname, String description, String icon, String defaultvalue, boolean optional) {
		addProperty(new PropertyPassword(name, displayname, description, icon, defaultvalue, optional));
	}

	public void addIntegerProperty(String name, String displayname, String description, String icon, Integer defaultvalue, boolean optional) {
		addProperty(new PropertyInt(name, displayname, description, icon, defaultvalue, optional));
	}

	public void addLongProperty(String name, String displayname, String description, String icon, Long defaultvalue, boolean optional) {
		addProperty(new PropertyLong(name, displayname, description, icon, defaultvalue, optional));
	}

	public void addStringProperty(String name, String displayname, String description, String icon, String defaultvalue, boolean optional) {
		addProperty(new PropertyString(name, displayname, description, icon, defaultvalue, optional));
	}

	public void addBooleanProperty(String name, String displayname, String description, String icon, Boolean defaultvalue, boolean optional) {
		addProperty(new PropertyBoolean(name, displayname, description, icon, defaultvalue, optional));
	}

	public void addArrayListProperty(String name, String displayname, String description, String icon, boolean optional) {
		addProperty(new PropertyArrayList(name, displayname, description, icon, null, null, optional));
	}

	public PropertyGroup addPropertyGroupProperty(String name, String displayname, String description, String icon, boolean optional) {
		PropertyGroup p = new PropertyGroup(name, displayname, description, icon, optional);
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

}
