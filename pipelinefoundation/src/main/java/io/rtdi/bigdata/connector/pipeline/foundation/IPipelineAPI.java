package io.rtdi.bigdata.connector.pipeline.foundation;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerMetadataEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.LoadInfo;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerMetadataEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ServiceEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ServiceMetadataEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;
import io.rtdi.bigdata.connector.properties.PipelineConnectionProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;
import io.rtdi.bigdata.connector.properties.ServiceProperties;

/**
 * A Pipeline has to implement all of these methods. 
 * It has to test for permissions to use these methods.
 * 
 *
 * @param <S> PipelineConnectionProperties
 * @param <T> TopicHandler
 * @param <P> ProducerSession
 * @param <C> ConsumerSession
 */
public interface IPipelineAPI<S extends PipelineConnectionProperties, T extends TopicHandler, P extends ProducerSession<T>, C extends ConsumerSession<T>> extends IPipelineBase<S, T> {

	String getAPIName();

	@Override
	Schema getSchema(int schemaid) throws PropertiesException;

	SchemaHandler registerSchema(SchemaRegistryName schemaname, String description, Schema keyschema, Schema valueschema) throws PropertiesException;

	/**
	 * @return A list of schema names for this tenant
	 * @throws PropertiesException if something goes wrong
	 */
	List<String> getSchemas() throws PropertiesException;

	/**
	 * Creates a new topic and fails if it exists already. see {@link #getTopicOrCreate(TopicName, int, short)}
	 * 
	 * @param topic TopicName
	 * @param partitioncount for the topic
	 * @param replicationfactor for the topic
	 * @param configs optional parameters
	 * @return TopicHandler
	 * @throws PropertiesException if something goes wrong
	 */
	T topicCreate(TopicName topic, int partitioncount, short replicationfactor, Map<String, String> configs) throws PropertiesException;

	/**
	 * Creates a new topic and fails if it exists already. see {@link #getTopicOrCreate(TopicName, int, short)}
	 * 
	 * @param topic TopicName
	 * @param partitioncount for the topic
	 * @param replicationfactor for the topic
	 * @return TopicHandler
	 * @throws PropertiesException if something goes wrong
	 */
	T topicCreate(TopicName topic, int partitioncount, short replicationfactor) throws PropertiesException;


	/**
	 * A synchronized version of the {@link #getTopic(TopicName)} and {@link #topicCreate(TopicName, int, short, Map)} to deal with the case where 
	 * two threads want to create the same topic at the same time.
	 * 
	 * @param name name of the topic within the tenant
	 * @param partitioncount for the topic
	 * @param replicationfactor for the topic
	 * @param configs Optional server specific properties
	 * @return TopicHandler
	 * @throws PropertiesException if something goes wrong
	 */
	T getTopicOrCreate(TopicName name, int partitioncount, short replicationfactor, Map<String, String> configs) throws PropertiesException;

	/**
	 * Simplified version of {@link #getTopicOrCreate(TopicName, int, short, Map)} for convenience.
	 * 
	 * @param topicname name of the topic within the tenant
	 * @param partitioncount for the topic
	 * @param replicationfactor for the topic
	 * @return TopicHandler
	 * @throws PropertiesException if something goes wrong
	 * 
	 */
	T getTopicOrCreate(TopicName topicname, int partitioncount, short replicationfactor) throws PropertiesException;

	SchemaHandler getOrCreateSchema(SchemaRegistryName name, String description, Schema keyschema, Schema valueschema) throws PropertiesException;

	/**
	 * Get the TopicHandler of an already existing topic.
	 * 
	 * @param topicname The tenant's name of the topic
	 * @return The TopicHandler representing the topic or null
	 * @throws PropertiesException if something goes wrong
	 */
	T getTopic(TopicName topicname) throws PropertiesException;

	/**
	 * Get a list of all known topics
	 * @return A list of all known topics in the server
	 * @throws IOException if something goes wrong
	 */
	List<String> getTopics() throws IOException;


	/**
	 * Read the most recent n records from a topic.
	 * 
	 * @param topicname TopicName
	 * @param count of most recent records to return
	 * @return List of AvroRecords and their metadata in descending order, most recent first!
	 * @throws IOException if something goes wrong
	 */
	List<TopicPayload> getLastRecords(TopicName topicname, int count) throws IOException;

	/**
	 * Read all records from a given timestamp onwards. Like {@link #getLastRecords(TopicName, int)} but based using a 
	 * timestamp as starting point and with the option to limit on one schema.
	 *  
	 * @param topicname TopicName
	 * @param timestamp starting point to read from
	 * @param count stop reading after this many records
	 * @param schema optional schema name to scan for
	 * @return List of AvroRecords and their metadata in ascending order, oldest first!
	 * @throws IOException if something goes wrong
	 */
	List<TopicPayload> getAllRecordsSince(TopicName topicname, long timestamp, int count, SchemaRegistryName schema) throws IOException;

	P createNewProducerSession(ProducerProperties properties) throws PropertiesException;

	C createNewConsumerSession(ConsumerProperties properties) throws PropertiesException;

	ServiceSession createNewServiceSession(ServiceProperties properties) throws PropertiesException;
	
	/**
	 * This method should implement everything that is needed to open the connection to the topic server and thus enables all other api methods.<br>
	 * The implementation should set useful information during the open sequence to enable the end user
	 * to see where the open method might have failed.
	 * 
	 * @throws IOException if something goes wrong
	 */
	void open() throws IOException;

	void close();

	/**
	 * @param producername to remove
	 * @throws IOException if something goes wrong
	 */
	void removeProducerMetadata(String producername) throws IOException;

	void removeConsumerMetadata(String consumername) throws IOException;
	
	void removeServiceMetadata(String consumername) throws IOException;

	/**
	 * Add the producer's information to the metadata directory
	 * 
	 * @param producer ProducerEntity to add metadata for
	 * @throws IOException if something goes wrong
	 */
	void addProducerMetadata(ProducerEntity producer) throws IOException;

	/**
	 * Add the consumer's information to the metadata directory 
	 * 
	 * @param consumer ConsumerEntity to add metadata for
	 * @throws IOException if something goes wrong
	 */
	void addConsumerMetadata(ConsumerEntity consumer) throws IOException;

	/**
	 * Add the service information to the metadata directory 
	 * 
	 * @param service ServiceEntity to add metadata for
	 * @throws IOException if something goes wrong
	 */
	void addServiceMetadata(ServiceEntity service) throws IOException;

	/**
	 * @return The information of all producers, the topics they use and the schemas of each topic
	 * @throws IOException if something goes wrong
	 */
	ProducerMetadataEntity getProducerMetadata() throws IOException;

	/**
	 * @return The list of all consumers and the topics they listen on
	 * @throws IOException if something goes wrong
	 */
	ConsumerMetadataEntity getConsumerMetadata() throws IOException;

	/**
	 * @return The list of all services and the topics they work on
	 * @throws IOException if something goes wrong
	 */
	ServiceMetadataEntity getServiceMetadata() throws IOException;

	S getAPIProperties();

	/**
	 * Find the properties file in the provided directory structure and load the contents so the {@link #open()} has all information it needs.
	 * 
	 * @throws PropertiesException in case properties are wrong
	 */
	public void loadConnectionProperties() throws PropertiesException;

	public void reloadConnectionProperties() throws IOException;

	/**
	 * Write the current connection properties into a file within the currently active root directory tree. <br>
	 * <br>
	 * Note: The root directory needs to be set at the point. Usually because the {@link #loadConnectionProperties()} was called and it remembers that directory.
	 * 
	 * @throws PropertiesException in case the properties cannot be written
	 * 
	 */
	public void writeConnectionProperties() throws PropertiesException;

	/**
	 * @return A short label identifying the target 
	 */
	public String getConnectionLabel();

	/**
	 * @return The hostname the JVM is running on
	 */
	public String getHostName();

	boolean hasConnectionProperties();

	void setWEBINFDir(File webinfdir);

	void validate() throws IOException;

	boolean isAlive();

	String getBackingServerConnectionLabel();
	
	/**
	 * Returns the list of all schemas that have been initial-loaded.
	 * If an entry was never initial loaded, then it is not present.
	 * 
	 * @param producername the name of the producer
	 * @param instanceno the producer instance number
	 * @return a Map<Schemaname, Map<ProducerInstanceNumber, InitialLoadInfo>>
	 * @throws IOException
	 */
	Map<String, LoadInfo> getLoadInfo(String producername, int instanceno) throws IOException;

}