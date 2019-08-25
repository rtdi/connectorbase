package io.rtdi.bigdata.connector.properties;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import io.rtdi.bigdata.connector.pipeline.foundation.MicroServiceTransformation;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.atomic.IProperty;
import io.rtdi.bigdata.connector.properties.atomic.PropertyGroup;
import io.rtdi.bigdata.connector.properties.atomic.PropertyRoot;

/**
 * The ServiceProperties is the class holding all service specific settings.
 * @param <M> MicroServiceTransformation
 *
 */
public abstract class ServiceProperties<M extends MicroServiceTransformation> {

	private static final String SOURCE = "service.source";
	private static final String TARGET = "service.target";
	protected PropertyRoot properties;
	private List<M> microservices = new ArrayList<>();
	private boolean isvalid = false;

	public ServiceProperties(String name) throws PropertiesException {
		super();
		properties = new PropertyRoot(name);
		properties.addStringProperty(SOURCE, "Source topic name", "Topic name the service consumes", null, null, true);
		properties.addStringProperty(TARGET, "Target topic name", "Topic name the service writes the results into", null, null, true);
	}
	
	public ServiceProperties(File dir, String name) throws PropertiesException {
		this(name);
		read(dir);
	}

	public void addMicroService(M microservice) {
		microservices.add(microservice);
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
	 * @param services lists all microservices
	 * @throws PropertiesException if one of the data types does not match
	 * 
	 * @see PropertyRoot#parseValue(PropertyRoot, boolean)
	 */
	public void setValue(PropertyRoot pg, List<String> services) throws PropertiesException {
		properties.parseValue(pg, false);
	}

	/**
	 * @return The backing PropertyGroup with all the values
	 */
	public PropertyRoot getPropertyGroup() {
		return properties;
	}

	public PropertyRoot getPropertyGroupNoPasswords() throws PropertiesException {
		PropertyRoot clone = new PropertyRoot(properties.getName());
		clone.parseValue(properties, true);
		return clone;
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
	 * @throws PropertiesException if the file cannot be read or the content is wrong
	 */
	public void read(File directory) throws PropertiesException {
		properties.read(directory);
		for (File step : directory.listFiles()) {
			if (step.isDirectory()) {
				M transformation = readMicroservice(step);
				addMicroService(transformation);
				isvalid = true;
			}
		}
	}
	
	protected abstract M readMicroservice(File dir) throws PropertiesException;

	/**
	 * Write the current properties into a directory. The file name is derived from the {@link #getName()}.
	 *  
	 * @param directory where to write the file to
	 * @throws PropertiesException if the file is not write-able
	 */
	public void write(File directory) throws PropertiesException {
		properties.write(directory);
		if (microservices != null) {
			for ( M m : microservices) {
				File step = new File(directory.getAbsolutePath() + File.separatorChar + m.getName());
				if (!step.exists()) {
					step.mkdir();
				}
				writeMicroservice(m, step);
			}
		}
	}
	
	protected abstract void writeMicroservice(M m, File dir) throws PropertiesException;

	public String getSourceTopic() {
		return properties.getStringPropertyValue(SOURCE);
	}

	public String getTargetTopic() {
		return properties.getStringPropertyValue(TARGET);
	}

	public void setSourceTopic(String value) throws PropertiesException {
		properties.setProperty(SOURCE, value);
	}

	public void setTargetTopic(String value) throws PropertiesException {
		properties.setProperty(TARGET, value);
	}

	public List<M> getMicroServices() {
		return microservices;
	}

	public boolean isValid() {
		return isvalid;
	}

}
