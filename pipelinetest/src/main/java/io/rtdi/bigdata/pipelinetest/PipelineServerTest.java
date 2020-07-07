package io.rtdi.bigdata.pipelinetest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.pipeline.foundation.PipelineServerAbstract;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaName;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicPayload;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerMetadataEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerMetadataEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ServiceEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ServiceMetadataEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;
import io.rtdi.bigdata.connector.properties.PipelineConnectionServerProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;

public class PipelineServerTest extends PipelineServerAbstract<PipelineConnectionServerProperties, TopicHandlerTest, ProducerSessionTest, ConsumerSessionTest> {

	private List<Schema> schemas = new ArrayList<>();
	private Map<SchemaName, SchemaHandler> schemahandlers = new HashMap<>();
	private Map<TopicName, TopicHandlerTest> topics = new HashMap<>();
	
	private ProducerMetadataEntity producerdirectory = null;
	private ConsumerMetadataEntity consumerdirectory = null;
	private ServiceMetadataEntity servicedirectory = null;
	
	public PipelineServerTest(PipelineConnectionServerProperties connectionproperties) {
		super(connectionproperties);
	}

	public PipelineServerTest(File rootdir) {
		super(new PipelineConnectionServerProperties("EMPTY"));
	}

	public PipelineServerTest() {
		super();
	}

	@Override
	public void loadConnectionProperties(File webinfdir) throws PropertiesException {
		setConnectionProperties(new PipelineConnectionServerProperties("EMPTY"));
	}


	@Override
	public void writeConnectionProperties(File webinfdir) throws PropertiesException {
	}

	@Override
	public Schema getSchema(int schemaid) throws PipelineRuntimeException {
		return schemas.get(schemaid);
	}

	@Override
	public SchemaHandler getSchema(SchemaName schemaname) throws PipelineRuntimeException {
		return schemahandlers.get(schemaname);
	}

	@Override
	public List<String> getSchemas() throws PipelineRuntimeException {
		ArrayList<String> ret = new ArrayList<>();
		for (SchemaName n : schemahandlers.keySet()) {
			ret.add(n.getName());
		}
		return ret;
	}

	@Override
	public SchemaHandler getOrCreateSchema(SchemaName name, String description, Schema keyschema, Schema valueschema) throws PropertiesException {
		SchemaHandler current = schemahandlers.get(name);
		if (current != null && current.getKeySchema().equals(keyschema) && current.getValueSchema().equals(valueschema)) {
			return current;
		}
		int k = schemas.size();
		schemas.add(keyschema);
		int v = schemas.size();
		schemas.add(valueschema);
		SchemaHandler h = new SchemaHandler(name, keyschema, valueschema, k, v);
		schemahandlers.put(name, h);
		return h;
	}

	@Override
	public TopicHandlerTest createTopic(TopicName topic, int partitioncount, short replicationfactor, Map<String, String> configs) throws PropertiesException {
		TopicHandlerTest t = topics.get(topic);
		if (t == null) {
			t = new TopicHandlerTest(topic, 1, 1);
			topics.put(topic, t);
		}
		return t;
	}

	@Override
	public TopicHandlerTest getTopic(TopicName topic) throws PipelineRuntimeException {
		return topics.get(topic);
	}

	@Override
	public List<String> getTopics() throws PipelineRuntimeException {
		ArrayList<String> list = new ArrayList<>();
		for (TopicName t : topics.keySet()) {
			list.add(t.getName());
		}
		return list;
	}

	@Override
	public List<TopicPayload> getLastRecords(TopicName topicname, int count) throws PipelineRuntimeException {
		TopicHandlerTest handler = getTopic(topicname);
		if (handler != null) {
			List<TopicPayload> data = handler.getData();
			List<TopicPayload> ret = new ArrayList<>();
			int limit = data.size() - count;
			for (int i=data.size()-1; i>=limit && i >= 0; i--) {
				ret.add(data.get(i));
			}
			return ret;
		} else {
			return null;
		}
	}

	@Override
	public List<TopicPayload> getLastRecords(TopicName topicname, long timestamp) throws PipelineRuntimeException {
		TopicHandlerTest handler = getTopic(topicname);
		if (handler != null) {
			List<TopicPayload> data = handler.getData();
			List<TopicPayload> ret = new ArrayList<>();
			for (int i=data.size()-1; i>0; i--) {
				TopicPayload t = data.get(i);
				if (t.getTimestamp() > timestamp) {
					ret.add(t);
				} else {
					break;
				}
			}
			return ret;
		} else {
			return null;
		}
	}

	@Override
	public void open() {
	}

	@Override
	public void close() {
	}

	@Override
	public void removeProducerMetadata(String producername) throws PipelineRuntimeException {
		if (producerdirectory != null) {
			producerdirectory.remove(producername);
		}
	}

	@Override
	public void removeConsumerMetadata(String consumername) throws PipelineRuntimeException {
		if (consumerdirectory != null) {
			consumerdirectory.remove(consumername);
		}
	}

	@Override
	public void removeServiceMetadata(String servicename) throws IOException {
		if (servicedirectory != null) {
			servicedirectory.remove(servicename);
		}
	}

	@Override
	public void addConsumerMetadata(ConsumerEntity consumer) throws IOException {
		if (consumerdirectory == null) {
			consumerdirectory = new ConsumerMetadataEntity();
		}
		consumerdirectory.update(consumer);
	}

	@Override
	public void addProducerMetadata(ProducerEntity producer) throws IOException {
		if (producerdirectory == null) {
			producerdirectory = new ProducerMetadataEntity();
		}
		producerdirectory.update(producer);
	}

	@Override
	public void addServiceMetadata(ServiceEntity service) throws IOException {
		if (servicedirectory == null) {
			servicedirectory = new ServiceMetadataEntity();
		}
		servicedirectory.update(service);
	}

	@Override
	public ProducerMetadataEntity getProducerMetadata() throws IOException {
		return producerdirectory;
	}

	@Override
	public ConsumerMetadataEntity getConsumerMetadata() throws IOException {
		return consumerdirectory;
	}

	@Override
	public ServiceMetadataEntity getServiceMetadata() throws IOException {
		return servicedirectory;
	}

	@Override
	public ProducerSessionTest createNewProducerSession() throws PropertiesException {
		return new ProducerSessionTest(new ProducerProperties("producer"), this);
	}

	@Override
	public ConsumerSessionTest createNewConsumerSession(String consumername, String topicpattern) throws PropertiesException {
		ConsumerProperties props = new ConsumerProperties(consumername);
		props.setTopicPattern(topicpattern);
		return new ConsumerSessionTest(props, this);
	}

	@Override
	public boolean isAlive() {
		return true;
	}

	@Override
	public String getConnectionLabel() {
		return null;
	}

}
