package io.rtdi.bigdata.connector.properties.atomic;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class PropertyRoot extends PropertyGroupAbstract {

	private static ObjectMapper mapper;
	private String description;
	
	static {
		mapper = new ObjectMapper();
	}
	
	public PropertyRoot() {
		super();
	}
	
	public PropertyRoot(String name) {
		super(name);
	}

	/**
	 * Read the entire properties from a file matching the name.
	 *  
	 * @param directory The directory where a file with the {@link #getName()} plus .json suffix is found
	 * @throws PropertiesException
	 */
	public void read(File directory) throws PropertiesException {
		if (!directory.exists()) {
			throw new PropertiesException("File \"" + directory + "\" does not exist");
		} else if (!directory.isDirectory()) {
			throw new PropertiesException("File \"" + directory + "\" is not a directory");
		} else { 
			File file = new File(directory.getAbsolutePath() + File.separatorChar + getName() + ".json");
			if (!file.canRead()) {
				throw new PropertiesException("File \"" + file + "\" is not read-able");
			} else {
				try {
				    PropertyRoot pg = mapper.readValue(file, PropertyRoot.class);
			        parseValue(pg);
				} catch (IOException e) {
					throw new PropertiesException("Cannot parse the json file with the properties", e, "check filename and format", file.getName());
				}
			}
		}
	}
	
	/**
	 * Write the current properties to a file with its name derived from {@link #getName()}.
	 * 
	 * @param directory The directory where a file with the {@link #getName()} plus .json suffix should be written into
	 * @throws PropertiesException
	 */
	public void write(File directory) throws PropertiesException {
		if (!directory.exists()) {
			throw new PropertiesException("File \"" + directory + "\" does not exist");
		} else if (!directory.isDirectory()) {
			throw new PropertiesException("File \"" + directory + "\" is not a directory");
		} else {
			File file = new File(directory.getAbsolutePath() + File.separatorChar + getName() + ".json");
			if (file.exists() && !file.canWrite()) { // Either the file does not exist or it exists and is write-able
				throw new PropertiesException("File \"" + file + "\" is not write-able");
			} else {
				/*
				 * When writing the properties to a file, all the extra elements like description etc should not be stored.
				 * Therefore a simplified version of the proeprty tree needs to be created.
				 */
				Simplified simplified = new Simplified(this);
				try {
	    			mapper.writeValue(file, simplified);
				} catch (IOException e) {
					throw new PropertiesException("Failed to write the json properties file", e, "check filename", file.getName());
				}
				
			}
		}
	}
	
	public void parseValue(PropertyRoot value) throws PropertiesException {
		if (value instanceof PropertyGroupAbstract) {
			PropertyGroupAbstract pg = (PropertyGroupAbstract) value;
			for (IProperty v : pg.getValues()) {
				if (v.getName() == null) {
					throw new PropertiesException("The passed element \"" + v.toString() + "\" does not have a name");
				}
				IProperty e = nameindex.get(v.getName());
				if (e == null) {
					nameindex.put(v.getName(), v);
					propertylist.add(v);
				} else {
					if (e instanceof PropertyGroup) {
						((PropertyGroup) e).parseValue(v);
					} else if (e instanceof IPropertyValue) {
						((IPropertyValue) e).parseValue(v);						
					}
				}
			}
			this.valuesset = true;
		} else {
			throw new PropertiesException("PropertyGroup value not of type PropertyGroup");
		}
	}

	
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	public static class Simplified {
		String type;
		String name;
		String value;
		ArrayList<Simplified> values;

		public Simplified() {
			super();
		}

		public Simplified(PropertyRoot pg) {
			super();
			values = new ArrayList<>();
			List<IProperty> l = (List<IProperty>) pg.getValues();
    		for (IProperty element : l) {
    			values.add(new Simplified(element));
    		}
		}

		public Simplified(IProperty element) {
			super();
			// getSimpleName() returns e.g. PropertyString but should be propertyString according to JAXB
			type = element.getClass().getSimpleName();
			name = element.getName();
			if (element instanceof PropertyGroup) {
				PropertyGroup pg = (PropertyGroup) element;
				values = new ArrayList<>();
				List<IProperty> l = (List<IProperty>) pg.getValues();
	    		for (IProperty e : l) {
	    			values.add(new Simplified(e));
	    		}
			} else if (element instanceof IPropertyValue) {
				Object obj = ((IPropertyValue) element).getValue();
				value = obj.toString(); // needed to write the password and not to masked value in case of a PropertyPassword
			}
		}
		
		public String getType() {
			return type;
		}
		public void setType(String type) {
			this.type = type;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getValue() {
			return value;
		}
		public void setValue(String value) {
			this.value = value;
		}
		public ArrayList<Simplified> getValues() {
			return values;
		}
		public void setValues(ArrayList<Simplified> values) {
			this.values = values;
		}
	}


	public String getName() {
		return name;
	}
	
	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

}
