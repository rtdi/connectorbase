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
	
	private Map<String, ProducerMetadataEntity> producerdirectory = new HashMap<>();
	private Map<String, ConsumerMetadataEntity> consumerdirectory = new HashMap<>();
	private Map<String, ServiceMetadataEntity> servicedirectory = new HashMap<>();
	
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
	public List<String> getSchemas(String tenantid) throws PipelineRuntimeException {
		ArrayList<String> ret = new ArrayList<>();
		for (SchemaName n : schemahandlers.keySet()) {
			if (n.getTenant().equals(tenantid)) {
				ret.add(n.getName());
			}
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
	public TopicHandlerTest createTopic(TopicName topic, int partitioncount, int replicationfactor, Map<String, String> configs) throws PropertiesException {
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
	public List<String> getTopics(String tenantid) throws PipelineRuntimeException {
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
	public void removeProducerMetadata(String tenantid, String producername) throws PipelineRuntimeException {
		ProducerMetadataEntity producers = producerdirectory.get(tenantid);
		if (producers != null) {
			producers.remove(producername);
		}
	}

	@Override
	public void removeConsumerMetadata(String tenantid, String consumername) throws PipelineRuntimeException {
		ConsumerMetadataEntity consumers = consumerdirectory.get(tenantid);
		if (consumers != null) {
			consumers.remove(consumername);
		}
	}

	@Override
	public void removeServiceMetadata(String tenantid, String servicename) throws IOException {
		ServiceMetadataEntity services = servicedirectory.get(tenantid);
		if (services != null) {
			services.remove(servicename);
		}
	}

	@Override
	public void addConsumerMetadata(String tenantid, ConsumerEntity consumer) throws IOException {
		ConsumerMetadataEntity consumers = consumerdirectory.get(tenantid);
		if (consumers == null) {
			consumers = new ConsumerMetadataEntity();
			consumerdirectory.put(tenantid, consumers);
		}
		consumers.update(consumer);
	}

	@Override
	public void addProducerMetadata(String tenantid, ProducerEntity producer) throws IOException {
		ProducerMetadataEntity producers = producerdirectory.get(tenantid);
		if (producers == null) {
			producers = new ProducerMetadataEntity();
			producerdirectory.put(tenantid, producers);
		}
		producers.update(producer);
	}

	@Override
	public void addServiceMetadata(String tenantid, ServiceEntity service) throws IOException {
		ServiceMetadataEntity services = servicedirectory.get(tenantid);
		if (services == null) {
			services = new ServiceMetadataEntity();
			servicedirectory.put(tenantid, services);
		}
		services.update(service);
	}

	@Override
	public ProducerMetadataEntity getProducerMetadata(String tenantid) throws IOException {
		return producerdirectory.get(tenantid);
	}

	@Override
	public ConsumerMetadataEntity getConsumerMetadata(String tenantid) throws IOException {
		return consumerdirectory.get(tenantid);
	}

	@Override
	public ServiceMetadataEntity getServiceMetadata(String tenantid) throws IOException {
		return servicedirectory.get(tenantid);
	}

	@Override
	public ProducerSessionTest createNewProducerSession(String tenantid) throws PropertiesException {
		return new ProducerSessionTest(new ProducerProperties("producer"), tenantid, this);
	}

	@Override
	public ConsumerSessionTest createNewConsumerSession(String consumername, String topicpattern, String tenantid) throws PropertiesException {
		ConsumerProperties props = new ConsumerProperties(consumername);
		props.setTopicPattern(topicpattern);
		return new ConsumerSessionTest(props, this, tenantid);
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
