package io.rtdi.bigdata.connector.properties;

import java.io.File;
import java.util.List;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.atomic.IProperty;
import io.rtdi.bigdata.connector.properties.atomic.PropertyGroup;
import io.rtdi.bigdata.connector.properties.atomic.PropertyRoot;

/**
 * The ProducerProperties is the class holding all producer specific settings. As the producer is a inner of 
 * the connection, the connection properties have to exist at that point.
 *
 */
public class ProducerProperties {
	public static final String PRODUCER_COUNT = "producer.instances";

	protected PropertyRoot properties;

	public ProducerProperties(String name) throws PropertiesException {
		super();
		/* if (name == null || name.length() == 0) {
			throw new PropertiesException("Producer properties must have a name");
		} */
		properties = new PropertyRoot(name);
		properties.addIntegerProperty(PRODUCER_COUNT, PRODUCER_COUNT, null, null, 1, false);
	}
	
	public ProducerProperties(File dir, String name) throws PropertiesException {
		this(name);
		this.read(dir);
	}

	/**
	 * @return The name of the Producer
	 */
	public String getName() {
		return properties.getName();
	}
	 
	/**
	 * @return The list of all properties
	 * @see PropertyGroup#getValues()
	 */
	public List<IProperty> getValue() {
		return properties.getValues();
	}
	
	/**
	 * Copy all values of the provided PropertyGroup into this object's values.
	 * 
	 * @param pg PropertyRoot to take the values from
	 * @throws PropertiesException if one of the data types does not match
	 * 
	 * @see PropertyRoot#parseValue(PropertyRoot)
	 */
	public void setValue(PropertyRoot pg) throws PropertiesException {
		properties.parseValue(pg);
	}

	/**
	 * @return The backing PropertyGroup with all the values
	 */
	public PropertyRoot getPropertyGroup() {
		return properties;
	}

	@Override
	public String toString() {
		if (properties != null) {
			return properties.toString();
		} else {
			return "NULL";
		}
	}

	/**
	 * Read the individual properties from a directory. The file name is derived from the {@link #getName()}.
	 *  
	 * @param directory where the file can be found
	 * @throws PropertiesException if the fiel cannot be read or the content is wrong
	 */
	public void read(File directory) throws PropertiesException {
		properties.read(directory);
	}
	
	/**
	 * Write the current properties into a directory. The file name is derived from the {@link #getName()}.
	 *  
	 * @param directory where to write the file to
	 * @throws PropertiesException if the file is not write-able
	 */
	public void write(File directory) throws PropertiesException {
		properties.write(directory);
	}
	
	/**
	 * Set the InstanceCount property value.
	 * 
	 * @param instances is the number of parallel producers
	 * @throws PropertiesException in case there is a data type mismatch
	 */
	public void setInstanceCount(int instances) throws PropertiesException {
		properties.setProperty(PRODUCER_COUNT, instances);
	}
	
	/**
	 * @return InstanceCount property value
	 */
	public Integer getInstanceCount() {
		return properties.getIntPropertyValue(PRODUCER_COUNT);
	}

}
