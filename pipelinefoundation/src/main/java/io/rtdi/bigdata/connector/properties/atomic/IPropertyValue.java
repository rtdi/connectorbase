package io.rtdi.bigdata.connector.properties.atomic;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public interface IPropertyValue extends IProperty, INamed {

	Object getValue();

	void parseValue(IProperty value, boolean ignorepasswords) throws PropertiesException;

	boolean hasValue();

}
