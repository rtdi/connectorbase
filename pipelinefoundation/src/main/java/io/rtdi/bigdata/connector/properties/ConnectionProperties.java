package io.rtdi.bigdata.connector.properties;

import java.io.File;
import java.util.List;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.atomic.IProperty;
import io.rtdi.bigdata.connector.properties.atomic.PropertyGroup;
import io.rtdi.bigdata.connector.properties.atomic.PropertyRoot;

/**
 * The design time object of the Connection. The actual data is all stored in a PropertyGroup.
 * This split is because the PropertyGroup has a JAXB reader/writer, this RemoteSourceProperties class does not.
 *
 */
public class ConnectionProperties {
	protected PropertyRoot properties;

	/**
	 * RemoteSourceProperties do have a unique name within an connector.
	 * 
	 * @param name of the properties
	 */
	public ConnectionProperties(String name) {
		super();
		properties = new PropertyRoot(name);
	}
		
	/**
	 * @return name of the RemoteSourceProperties
	 */
	public String getName() {
		return properties.getName();
	}

	/**
	 * Helper method to return a named value of the RemoteSourceProperties object.
	 * @see PropertyGroup#getValues()
	 * 
	 * @return List of all properties
	 */
	public List<IProperty> getValue() {
		return properties.getValues();
	}
		
	/**
	 * Helper method to set all named values of the RemoteSourceProperties object via a PropertyGroup.
	 * 
	 * @param pg set all values based on the contents of this PropertyRoot object
	 * @throws PropertiesException if one of the properties in invalid
	 */
	public void setValue(PropertyRoot pg) throws PropertiesException {
		properties.parseValue(pg);
	}

	
	/**
	 * @return The backing PropertyGroup containing the actual values
	 */
	public PropertyRoot getPropertyGroup() {
		return properties;
	}

	@Override
	public String toString() {
		return properties.toString();
	}

	/**
	 * Helper method to set the RemoteSourceProperties description into the backing PropertyGroup.
	 * 
	 * @param description The long text description of the RemoteSourceProperties object
	 */
	public void setDescription(String description) {
		properties.setDescription(description);
	}

	/**
	 * Helper method to get the RemoteSourceProperties description from the backing PropertyGroup.
	 * 
	 * @return description string
	 */
	public String getDescription() {
		return properties.getDescription();
	}

	/**
	 * Read the individual connection properties from a directory. The file name is derived from the {@link #getName()}.
	 * @param directory of the properties file
	 * @throws PropertiesException if the file has invalid contents
	 */
	public void read(File directory) throws PropertiesException {
		properties.read(directory);
	}
	
	/**
	 * Write the current connection properties into a directory. The file name is derived from the {@link #getName()}.
	 *  
	 * @param directory of the properties file
	 * @throws PropertiesException if one of the properties is invalid or the file is not write-able
	 */
	public void write(File directory) throws PropertiesException {
		properties.write(directory);
	}
}
