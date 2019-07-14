package io.rtdi.bigdata.connector.pipeline.foundation;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerMetadataEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerMetadataEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.TopicPayload;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;
import io.rtdi.bigdata.connector.properties.PipelineConnectionProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;

/**
 * A Pipeline has to implement all of these methods. 
 * It has to test for permissions to use these methods, has to be tenant-aware.
 * 
 * @see IPipelineServer for the interface dealing with multiple tenants at once
 * @see PipelineAbstract for a class redirecting all calls to the IPipelineServer
 *
 * @param <S> PipelineConnectionProperties
 * @param <T> TopicHandler
 * @param <P> ProducerSession
 * @param <C> ConsumerSession
 */
public interface IPipelineAPI<S extends PipelineConnectionProperties, T extends TopicHandler, P extends ProducerSession<T>, C extends ConsumerSession<T>> extends IPipelineBase<T> {

	/**
	 * Same as {@link #getSchema(SchemaName)} except that it accepts as input a string. Should return the same as 
	 * getSchema(new SchemaName(api.genTenantID(), schemaname))
	 * 
	 * @param schemaname The schema name of this tenant. It is not the FQN.
	 * @return schemahandler or null if not found
	 * @throws PropertiesException 
	 */
	SchemaHandler getSchema(String schemaname) throws PropertiesException;

	@Override
	Schema getSchema(int schemaid) throws PropertiesException;

	SchemaHandler registerSchema(SchemaName schemaname, String description, Schema keyschema, Schema valueschema) throws PropertiesException;

	SchemaHandler registerSchema(String schemaname, String description, Schema keyschema, Schema valueschema) throws PropertiesException;

	/**
	 * @return A list of schema names for this tenant
	 * @throws PropertiesException 
	 */
	List<String> getSchemas() throws PropertiesException;

	T topicCreate(TopicName topic, int partitioncount, int replicationfactor, Map<String, String> configs) throws PropertiesException;

	T topicCreate(TopicName topic, int partitioncount, int replicationfactor) throws PropertiesException;

	T topicCreate(String topic, int partitioncount, int replicationfactor, Map<String, String> configs) throws PropertiesException;

	T topicCreate(String topic, int partitioncount, int replicationfactor) throws PropertiesException;

	/**
	 * A synchronized version of the {@link #getTopic(String)} and {@link #topicCreate(TopicName, int, int, Map)} to deal with the case where 
	 * two threads want to create the same topic at the same time.
	 * 
	 * @param name
	 * @param partitioncount
	 * @param replicationfactor
	 * @param configs Optional server specific properties
	 * @return
	 * @throws PropertiesException 
	 */
	T getTopicOrCreate(String name, int partitioncount, int replicationfactor, Map<String, String> configs) throws PropertiesException;

	T getTopicOrCreate(String topicname, int partitioncount, int replicationfactor) throws PropertiesException;

	/**
	 * Get the TopicHandler of an already existing topic
	 * @param topicname The tenant's name of the topic
	 * @return The TopicHandler representing the topic or null
	 * @throws PropertiesException 
	 */
	T getTopic(String topicname) throws PropertiesException;

	/**
	 * Get a list of all known topics
	 * @return A list of all known topics in the server
	 * @throws PipelineRuntimeException
	 * @throws IOException 
	 */
	List<String> getTopics() throws PipelineRuntimeException, IOException;

	List<TopicPayload> getLastRecords(String topicname, int count) throws IOException;

	/**
	 * see @link {@link #getLastRecords(TopicName, int)} <BR/>
	 * Like above but used to re-read all data from a given timestamp onwards.
	 * @throws PropertiesException 
	 * @throws IOException 
	 */
	List<TopicPayload> getLastRecords(String topicname, long timestamp) throws IOException;

	List<TopicPayload> getLastRecords(TopicName topicname, int count) throws IOException;

	List<TopicPayload> getLastRecords(TopicName topicname, long timestamp) throws IOException;

	/**
	 * @return The tenantid as being set in the constructor
	 * @throws PropertiesException 
	 */
	String getTenantID() throws PropertiesException;

	P createNewProducerSession(ProducerProperties properties) throws PropertiesException;

	C createNewConsumerSession(ConsumerProperties properties) throws PropertiesException;

	/**
	 * This method should implement everything that is needed to open the connection to the topic server and thus enables all other api methods.<BR/>
	 * The implementation should set useful information during the open sequence using the {@link #setApistate(String)} method to enable the end user
	 * to see where the open method might have failed.
	 * 
	 * @throws PipelineRuntimeException
	 */
	void open() throws IOException;

	void close();

	void removeProducerMetadata(String producername) throws IOException;

	void removeConsumerMetadata(String consumername) throws IOException;

	/**
	 * Loops through the entire metadata tree and calls {@link #addProducerMetadata(String, TopicName, SchemaName)} for each.<BR/>
	 * Overwrite with a more effective implementation if applicable.
	 * 
	 * @param producer
	 * @throws IOException 
	 */
	void addProducerMetadata(ProducerEntity producer) throws IOException;

	/**
	 * Loops through the topics and calls {@link #addConsumerMetadata(String, TopicName)} for each.<BR/>
	 * Overwrite with a more effective implementation if applicable.
	 * 
	 * @param consumer
	 * @throws IOException 
	 */
	void addConsumerMetadata(ConsumerEntity consumer) throws IOException;

	/**
	 * @return The information of all producers, the topics they use and the schemas of each topic
	 * @throws PipelineRuntimeException
	 */
	ProducerMetadataEntity getProducerMetadata() throws IOException;

	/**
	 * @return The list of all consumers and the topics they listen on
	 * @throws IOException 
	 */
	ConsumerMetadataEntity getConsumerMetadata() throws IOException;

	S getAPIProperties();

	public void loadConnectionProperties(File webinfdir) throws PropertiesException;

	public void writeConnectionProperties() throws PropertiesException;

	/**
	 * @return A short label identifying the target 
	 */
	public String getConnectionLabel();

	/**
	 * @return The hostname the JVM is running on
	 */
	public String getHostName();

}