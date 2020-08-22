package io.rtdi.bigdata.connector.pipeline.foundation;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;

/**
 * The getSchema/getTopic
 * @param <S> ConnectionProperties
 * @param <T> TopicHandler
 */
public interface IPipelineBase<S extends ConnectionProperties, T extends TopicHandler> extends ISchemaRegistrySource {

	/**
	 * Get the TopicHandler of an already existing topic
	 * 
	 * @param topic The globally unique TopicName
	 * @return The TopicHandler representing the topic or null
	 * @throws PropertiesException if something goes wrong
	 */
	T getTopic(TopicName topic) throws PropertiesException;

	/**
	 * The method is used to get an existing schema by the SchemaName reference
	 * 
	 * @param schemaname Lookup the schema dictionary for a schema of the given SchemaName
	 * @return A global handler object containing all important information about the schema. 
	 * @throws PropertiesException if something goes wrong
	 */
	SchemaHandler getSchema(SchemaRegistryName schemaname) throws PropertiesException;

	S getAPIProperties();

}
