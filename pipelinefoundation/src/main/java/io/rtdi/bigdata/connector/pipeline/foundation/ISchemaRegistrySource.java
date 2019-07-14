package io.rtdi.bigdata.connector.pipeline.foundation;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

/**
 * Concrete implementation on how to get a schema from the schema registry.
 *
 */
public interface ISchemaRegistrySource {

	/**
	 * get schema from the schema registry
	 * 
	 * @param schemaid
	 * @return
	 * @throws PropertiesException 
	 */
	Schema getSchema(int schemaid) throws PropertiesException;

}
