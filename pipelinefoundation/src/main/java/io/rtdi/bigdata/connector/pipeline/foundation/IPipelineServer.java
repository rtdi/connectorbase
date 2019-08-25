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
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ServiceEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ServiceMetadataEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;

/**
 * This interface provides methods for a server side implementation of the API, where the API calls include the tenant information
 * instead of being for a single tenant only.
 *
 * @param <S> ConnectionProperties
 * @param <T> TopicHandler
 * @param <P> ProducerSession
 * @param <C> ConsumerSession
 */
public interface IPipelineServer<S extends ConnectionProperties, T extends TopicHandler, P extends ProducerSession<T>, C extends ConsumerSession<T>> extends IPipelineBase<S, T> {

	public void setConnectionProperties(S properties);
	
	List<String> getSchemas(String tenantid) throws PipelineRuntimeException;

	/**
	 * To create or update an existing schema with all its metadata.
	 * 
	 * @param name name of the schema
	 * @param description optional description
	 * @param keyschema Avro key-schema used by this
	 * @param valueschema Avro value-schema used by this 
	 * @return SchemaHandler
	 * @throws PropertiesException if something goes wrong
	 */
	SchemaHandler getOrCreateSchema(SchemaName name, String description, Schema keyschema, Schema valueschema) throws PropertiesException;

	/**
	 * Create a new topic  with the provided metadata
	 * 
	 * @param topic TopicName to be used - has to be unique
	 * @param partitioncount specifies how many partitions this topic should have
	 * @param replicationfactor specifies how often it should be replicated in case of a cluster
	 * @param configs optional server specific properties
	 * @return TopicHandler representing the topic with all its metadata
	 * @throws PropertiesException if something goes wrong
	 */
	T createTopic(TopicName topic, int partitioncount, int replicationfactor, Map<String, String> configs) throws PropertiesException;

	T topicCreate(TopicName topic, int partitioncount, int replicationfactor) throws PropertiesException;

	/**
	 * A synchronized version of the {@link #getTopic(TopicName)} and {@link #createTopic(TopicName, int, int, Map)} to deal with the case where 
	 * two threads want to create the same topic at the same time.
	 * 
	 * @param topic TopicName
	 * @param partitioncount for the topic
	 * @param replicationfactor for the topic
	 * @param configs optional server specific properties
	 * @return TopicHandler
	 * @throws PropertiesException if something goes wrong
	 */
	T getTopicOrCreate(TopicName topic, int partitioncount, int replicationfactor, Map<String, String> configs) throws PropertiesException;

	/**
	 * A synchronized version of the {@link #getTopic(TopicName)} and {@link #createTopic(TopicName, int, int, Map)} to deal with the case where 
	 * two threads want to create the same topic at the same time.
	 * 
	 * @param topic TopicName
	 * @param partitioncount for the topic
	 * @param replicationfactor for the topic
	 * @return TopicHandler
	 * @throws PropertiesException if something goes wrong
	 */
	T getTopicOrCreate(TopicName topic, int partitioncount, int replicationfactor) throws PropertiesException;

	List<String> getTopics(String tenantid) throws IOException;

	/**
	 * This method can be used to re-read the last n records in a topic without impacting the consumption. Useful for
	 * data preview and the such.<br>
	 * Should return the data sorted descending by insertion time
	 * 
	 * @param topicname The name of the topic to read data from
	 * @param count The number of records 
	 * @return list of Avro records with metadata
	 * @throws IOException if something goes wrong
	 */
	List<TopicPayload> getLastRecords(TopicName topicname, int count) throws IOException;

	List<TopicPayload> getLastRecords(TopicName topicname, long timestamp) throws IOException;

	void open() throws PropertiesException;

	void close();

	void removeProducerMetadata(String tenantid, String producername) throws IOException;

	void removeConsumerMetadata(String tenantid, String consumername) throws IOException;

	void removeServiceMetadata(String tenantid, String servicename) throws IOException;

	ProducerMetadataEntity getProducerMetadata(String tenantid) throws IOException;

	ConsumerMetadataEntity getConsumerMetadata(String tenantid) throws IOException;
	
	ServiceMetadataEntity getServiceMetadata(String tenantid) throws IOException;

	S getAPIProperties();

	void addProducerMetadata(String tenantid, ProducerEntity producer) throws IOException;

	void addConsumerMetadata(String tenantid, ConsumerEntity consumer) throws IOException;

	void addServiceMetadata(String tenantid, ServiceEntity service) throws IOException;

	public void loadConnectionProperties(File webinfdir) throws PropertiesException;

	public void writeConnectionProperties(File webinfdir) throws PropertiesException;

	P createNewProducerSession(String tenantid) throws PropertiesException;

	C createNewConsumerSession(String consumername, String topicpattern, String tenantid) throws PropertiesException;

	public boolean isAlive();

	/**
	 * @return null if there is no server connected to this server
	 */
	String getConnectionLabel();

	
}