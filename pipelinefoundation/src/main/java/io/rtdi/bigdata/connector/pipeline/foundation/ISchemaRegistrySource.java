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
	 * @param schemaid of the schema to fetch
	 * @return Avro Schema
	 * @throws PropertiesException if something goes wrong
	 */
	Schema getSchema(int schemaid) throws PropertiesException;

}
