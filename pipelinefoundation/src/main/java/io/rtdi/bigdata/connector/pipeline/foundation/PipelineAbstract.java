package io.rtdi.bigdata.connector.pipeline.foundation;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.KeySchema;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.ValueSchema;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.IOUtils;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;
import io.rtdi.bigdata.connector.properties.PipelineConnectionProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;

/**
 * The main API definition. Every concrete implementation of a transaction log service is based on this. <br>
 * Each instance can be used for a single tenant only.
 *
 * @param <S> PipelineConnectionProperties
 * @param <T> TopicHandler
 * @param <P> ProducerSession
 * @param <C> ConsumerSession
 */
public abstract class PipelineAbstract<
				S extends PipelineConnectionProperties, 
				T extends TopicHandler, 
				P extends ProducerSession<T>, 
				C extends ConsumerSession<T>> implements Closeable, IPipelineAPI<S, T, P, C> {

	protected File webinfdir;
	protected Logger logger = LogManager.getLogger(this.getClass().getName());

	public PipelineAbstract() {
		super();
	}
	
	@Override
	public SchemaHandler registerSchema(ValueSchema schema) throws PropertiesException {
		try {
			return registerSchema(schema.getFullName(), schema.getDescription(), KeySchema.create(schema.getSchema()), schema.getSchema());
		} catch (SchemaException e) {
			throw new PropertiesException("Cannot create the Avro schema out of the ValueSchema", e);
		}
	}

	@Override
	public boolean hasConnectionProperties() {
		Path p = webinfdir.toPath().resolve(this.getAPIName() + ".json");
		File f = p.toFile();
		return f.canRead();
	}


	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI#getTopicOrCreate(java.lang.String, int, int)
	 */
	@Override
	public final T getTopicOrCreate(String topicname, int partitioncount, short replicationfactor) throws PropertiesException {
		return getTopicOrCreate(topicname, partitioncount, replicationfactor, null);
	}

	public abstract void setConnectionProperties(S props);

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI#createNewProducerSession(io.rtdi.bigdata.connector.properties.ProducerProperties)
	 */
	@Override
	public P createNewProducerSession(ProducerProperties properties) throws PropertiesException {
		if (properties == null) {
			throw new PipelineCallerException("ProducerSession requires a ProducerProperties object to get its name");
		} else {
			return createProducerSession(properties);
		}
	}
		
	@Override
	public T getTopic(String name) throws PropertiesException {
		return getTopic(new TopicName(name));
	}

	@Override
	public SchemaHandler getSchema(String schemaname) throws PropertiesException {
		return getSchema(new SchemaName(schemaname));
	}

	/**
	 * Create a new ProducerSession based on the provided properties. <br>
	 * This method should not throw exceptions as it creates the object only.
	 * 
	 * @param properties Producer specific properties or null
	 * @return A new ProducerSession to be used for connecting against the server and producing records
	 * @throws PropertiesException if something wrong with the properties
	 */
	protected abstract P createProducerSession(ProducerProperties properties) throws PropertiesException;
	
	
	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI#createNewConsumerSession(io.rtdi.bigdata.connector.properties.ConsumerProperties)
	 */
	@Override
	public C createNewConsumerSession(ConsumerProperties properties) throws PropertiesException {
		if (properties == null) {
			throw new PropertiesException("ProducerSession requires a ProducerProperties object to get its name");
		} else {
			return createConsumerSession(properties);
		}
	}

	/**
	 * This factory method creates a new ConsumerSession object. <br>
	 * It should also add the concrete topics it does listen on. <br>
	 * This method should not throw exceptions as it creates the object only.
	 * 
	 * @param properties Mandatory parameter as it includes the topics to listen on
	 * @return A new ConsumerSession
	 * @throws PropertiesException if something goes wrong
	 */
	protected abstract C createConsumerSession(ConsumerProperties properties) throws PropertiesException;
	
	
	@Override
	public String getHostName() {
		return IOUtils.getHostname();
	}

	@Override
	public void setWEBINFDir(File webinfdir) {
		this.webinfdir = webinfdir;
	}
	
	public synchronized T getTopicOrCreate(TopicName topic, int partitioncount, short replicationfactor, Map<String, String> configs) throws PropertiesException {
		T t = getTopic(topic);
		if (t == null) {
			t = topicCreate(topic, replicationfactor, replicationfactor, configs);
		} 
		return t;
	}
	
	@Override
	public T getTopicOrCreate(String name, int partitioncount, short replicationfactor, Map<String, String> configs) throws PropertiesException {
		return getTopicOrCreate(new TopicName(name), partitioncount, replicationfactor, configs);
	}

	public T getTopicOrCreate(TopicName topic, int partitioncount, short replicationfactor) throws PropertiesException {
		return getTopicOrCreate(topic, partitioncount, replicationfactor, null);
	}

	@Override
	public T topicCreate(String topic, int partitioncount, short replicationfactor,
			Map<String, String> configs) throws PropertiesException {
		return topicCreate(new TopicName(topic), partitioncount, replicationfactor, configs);
	}

	@Override
	public T topicCreate(TopicName topic, int partitioncount, short replicationfactor)
			throws PropertiesException {
		return topicCreate(topic, partitioncount, replicationfactor, null);
	}

	@Override
	public T topicCreate(String topic, int partitioncount, short replicationfactor) throws PropertiesException {
		return topicCreate(new TopicName(topic), partitioncount, replicationfactor, null);
	}

	@Override
	public List<TopicPayload> getLastRecords(String topicname, int count) throws IOException {
		return getLastRecords(new TopicName(topicname), count);
	}

	@Override
	public List<TopicPayload> getLastRecords(String topicname, long timestamp, int count, String schema) throws IOException {
		return getLastRecords(new TopicName(topicname), timestamp, count, new SchemaName(schema));
	}

	@Override
	public SchemaHandler registerSchema(String schemaname, String description, Schema keyschema, Schema valueschema) throws PropertiesException {
		return registerSchema(new SchemaName(schemaname), description, keyschema, valueschema);
	}

	@Override
	public void reloadConnectionProperties() throws IOException {
		close();
		loadConnectionProperties();
		open();
	}

}
