package io.rtdi.bigdata.connector.pipeline.foundation;

import java.io.Closeable;
import java.util.Map;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.PipelineConnectionServerProperties;

/**
 * Maybe a good start to implement the Server Pipeline API
 *
 * @param <S> PipelineConnectionServerProperties
 * @param <T> TopicHandler
 * @param <P> ProducerSession
 * @param <C> ConsumerSession
 */
public abstract class PipelineServerAbstract<S extends PipelineConnectionServerProperties, T extends TopicHandler, P extends ProducerSession<T>, C extends ConsumerSession<T>> implements Closeable, IPipelineServer<S, T, P, C> {

	private S connectionproperties;

	public PipelineServerAbstract(S properties) {
		super();
		this.connectionproperties = properties;
	}
	
	public PipelineServerAbstract() {
	}

	@Override
	public void setConnectionProperties(S properties) {
		this.connectionproperties = properties;
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.pipeline.foundation.IPipelineServer#topicCreate(io.rtdi.bigdata.connector.pipeline.foundation.TopicName, int, int)
	 */
	@Override
	public T topicCreate(TopicName topic, int partitioncount, int replicationfactor) throws PropertiesException {
		return createTopic(topic, partitioncount, replicationfactor, null);
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.pipeline.foundation.IPipelineServer#getTopicOrCreate(io.rtdi.bigdata.connector.pipeline.foundation.TopicName, int, int, java.util.Map)
	 */
	@Override
	public synchronized T getTopicOrCreate(TopicName topic, int partitioncount, int replicationfactor, Map<String, String> configs) throws PropertiesException {
		T t = getTopic(topic);
		if (t == null) {
			t= createTopic(topic, replicationfactor, replicationfactor, configs);
		} 
		return t;
	}

	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.pipeline.foundation.IPipelineServer#getTopicOrCreate(io.rtdi.bigdata.connector.pipeline.foundation.TopicName, int, int)
	 */
	@Override
	public T getTopicOrCreate(TopicName topic, int partitioncount, int replicationfactor) throws PropertiesException {
		return getTopicOrCreate(topic, partitioncount, replicationfactor, null);
	}
	
	/* (non-Javadoc)
	 * @see io.rtdi.bigdata.connector.pipeline.foundation.IPipelineServer#getAPIProperties()
	 */
	@Override
	public S getAPIProperties() {
		return connectionproperties;
	}

}
