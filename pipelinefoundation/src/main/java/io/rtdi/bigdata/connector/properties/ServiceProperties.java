package io.rtdi.bigdata.connector.properties;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.rtdi.bigdata.connector.pipeline.foundation.MicroServiceTransformation;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ServiceConfigEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ServiceConfigEntity.ServiceSchema;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ServiceConfigEntity.ServiceStep;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.atomic.IProperty;
import io.rtdi.bigdata.connector.properties.atomic.PropertyGroup;
import io.rtdi.bigdata.connector.properties.atomic.PropertyRoot;

/**
 * The ServiceProperties is the class holding all service specific settings.
 *
 */
public abstract class ServiceProperties {

	private static final String SOURCE = "service.source";
	private static final String TARGET = "service.target";
	protected PropertyRoot properties;
	private Map<String, List<? extends MicroServiceTransformation>> schematransformations = new HashMap<>();
	private boolean isvalid = false;

	public ServiceProperties(String name) throws PropertiesException {
		super();
		properties = new PropertyRoot(name);
		properties.addTopicSelector(SOURCE, "Source topic name", "Topic name the service consumes", null, null, true);
		properties.addTopicSelector(TARGET, "Target topic name", "Topic name the service writes the results into", null, null, true);
	}
	
	public ServiceProperties(File dir, String name) throws PropertiesException {
		this(name);
		read(dir);
	}

	public void addMicroService(String schemaname, MicroServiceTransformation service) {
		@SuppressWarnings("unchecked")
		List<MicroServiceTransformation> l = (List<MicroServiceTransformation>) schematransformations.get(schemaname);
		if (l == null) {
			l = new ArrayList<>();
			schematransformations.put(schemaname, l);
		}
		l.add(service);
		l.sort((o1, o2)->o1.getName().compareTo(o2.getName()));
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
	 * @see PropertyRoot#parseValue(PropertyRoot, boolean)
	 */
	public void setValue(PropertyRoot pg) throws PropertiesException {
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
		for (File schemafile : directory.listFiles()) {
			if (schemafile.isDirectory()) {
				String schemaname = schemafile.getName();
				for (File step : schemafile.listFiles()) {
					if (step.isDirectory()) {
						MicroServiceTransformation transformation = readMicroservice(step);
						if (transformation != null) {
							addMicroService(schemaname, transformation);
							isvalid = true;
						}
					}
				}
			}
		}
	}
	
	protected abstract MicroServiceTransformation readMicroservice(File dir) throws PropertiesException;

	/**
	 * Write the current properties into a directory. The file name is derived from the {@link #getName()}.
	 *  
	 * @param directory where to write the file to
	 * @param data The requested directory structure
	 * @throws IOException In case the file tree cannot be built
	 */
	public void write(File directory, ServiceConfigEntity data) throws IOException {
		properties.write(directory);
		if (data != null) {
			Set<String> existingschemas = new HashSet<>();
			for (File schemadir : directory.listFiles()) {
				if (schemadir.isDirectory()) {
					existingschemas.add(schemadir.getName());
				}
			}
			// data provides a list of elements that might have been added or removed
			for (ServiceSchema s : data.getSchemas()) {
				File schemadir = new File(directory, s.getSchemaname());
				if (existingschemas.contains(s.getSchemaname())) {
					// Schema dir exists and is contained in the data 
					existingschemas.remove(s.getSchemaname());
				} else {
					// schema dir does not exist so must be a new one
					schemadir.mkdir();
				}
				
				// Check the transformation step sub directories
				Set<String> existingsteps = new HashSet<>();
				for (File stepdir : schemadir.listFiles()) {
					if (stepdir.isDirectory()) {
						existingsteps.add(stepdir.getName());
					}
				}
				for (ServiceStep step : s.getSteps()) {
					File stepdir = new File(schemadir, step.getStepname());
					if (existingsteps.contains(step.getStepname())) {
						// Step dir exists and is contained in the data 
						existingsteps.remove(step.getStepname());
					} else {
						// step dir does not exist so must be a new one
						stepdir.mkdir();
					}
				}
				for (String delete : existingsteps) {
					File stepdir = new File(schemadir, delete);
					deleteDir(stepdir.toPath());
				}					
			}
			for (String delete : existingschemas) {
				File schemadir = new File(directory, delete);
				deleteDir(schemadir.toPath());
			}
		}
	}
	
	private void deleteDir(Path path) throws IOException {
		Files.walk(path)
		.sorted(Comparator.reverseOrder())
		.map(Path::toFile)
		.forEach(File::delete);
	}
	
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

	public List<? extends MicroServiceTransformation> getMicroServices(String schemaname) {
		return schematransformations.get(schemaname);
	}

	public boolean isValid() {
		return isvalid;
	}

	public Map<String, List<? extends MicroServiceTransformation>> getSchemaTransformations() {
		return schematransformations;
	}
}
