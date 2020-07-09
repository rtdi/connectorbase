package io.rtdi.bigdata.pipelinehttp;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.avro.Schema;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.jackson.JacksonFeature;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.rtdi.bigdata.connector.pipeline.foundation.PipelineAbstract;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaName;
import io.rtdi.bigdata.connector.pipeline.foundation.ServiceSession;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicPayload;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerMetadataEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.JAXBErrorMessage;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerMetadataEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.SchemaEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.SchemaHandlerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.SchemaListEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ServiceEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ServiceMetadataEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.TopicHandlerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.TopicHandlerEntity.ConfigPair;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.TopicListEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.TopicPayloadBinaryData;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineTemporaryException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;
import io.rtdi.bigdata.connector.pipeline.foundation.metadata.subelements.TopicMetadata;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.KeySchema;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.ValueSchema;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.HttpUtil;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.IOUtils;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;
import io.rtdi.bigdata.connector.properties.ServiceProperties;


public class PipelineHttp extends PipelineAbstract<ConnectionPropertiesHttp, TopicHandlerHttp, ProducerSessionHttp, ConsumerSessionHttp> {
	private WebTarget target;
	private ConnectionPropertiesHttp properties = new ConnectionPropertiesHttp();
	protected URI transactionendpointsend;
	protected URI transactionendpointfetch;
	private File webinfdir = null;
	
	/* 
	 * These caches are used for internal operations only, e.g. getLastRecords().
	 * Regular consumers and producers want to have tighter control and cache the data themselves.
	 */
	private Cache<Integer, Schema> schemaidcache = Caffeine.newBuilder().expireAfterAccess(Duration.ofMinutes(30)).maximumSize(1000).build();


	public PipelineHttp(ConnectionPropertiesHttp props) throws PropertiesException {
		super();
		this.properties = props;
	}

	public PipelineHttp() throws PropertiesException {
		super();
	}
	
	@Override
	public SchemaHandler getSchema(String schemaname) throws PropertiesException {
		Response entityresponse = callRestfulservice(getRestEndpoint("/schema/byname", schemaname));
		if (entityresponse == null) {
			return null;
		} else {
			SchemaHandlerEntity entityout = entityresponse.readEntity(SchemaHandlerEntity.class);
			return new SchemaHandler(schemaname, entityout.getKeySchema(), entityout.getValueSchema(), entityout.getKeySchemaId(), entityout.getValueSchemaId());
		}
	}

	@Override
	public Schema getSchema(int schemaid) throws PropertiesException {
		Response entityresponse = callRestfulservice(getRestEndpoint("/schema/byid", String.valueOf(schemaid)));
		if (entityresponse == null) {
			return null;
		} else {
			SchemaEntity entityout = entityresponse.readEntity(SchemaEntity.class);
			return entityout.getSchema();
		}
	}

	@Override
	public SchemaHandler getSchema(SchemaName schemaname) throws PropertiesException {
		return getSchema(schemaname.getName());
	}

	@Override
	public SchemaHandler registerSchema(SchemaName schemaname, String description, Schema keyschema, Schema valueschema) throws PropertiesException {
		return registerSchema(schemaname.getName(), description, keyschema, valueschema);
	}

	@Override
	public SchemaHandler registerSchema(String schemaname, String description, Schema keyschema, Schema valueschema) throws PropertiesException {
		SchemaHandlerEntity entityin = new SchemaHandlerEntity();
		entityin.setSchemaName(schemaname);
		entityin.setDescription(description);
		entityin.setKeySchemaString(keyschema.toString());
		entityin.setValueSchemaString(valueschema.toString());
		Response entityresponse = postRestfulService(getRestEndpoint("/schema/byname", schemaname), entityin);
		SchemaHandlerEntity entityout = entityresponse.readEntity(SchemaHandlerEntity.class);
		return new SchemaHandler(schemaname, keyschema, valueschema, entityout.getKeySchemaId(), entityout.getValueSchemaId());
	}

	@Override
	public SchemaHandler registerSchema(ValueSchema schema) throws PropertiesException {
		try {
			return registerSchema(schema.getName(), schema.getDescription(), KeySchema.create(schema.getSchema()), schema.getSchema());
		} catch (SchemaException e) {
			throw new PropertiesException("Cannot create the Avro schema out of the ValueSchema", e);
		}
	}

	@Override
	public List<String> getSchemas() throws PropertiesException {
		Response entityresponse = callRestfulservice(getRestEndpoint("/schema", "list"));
		if (entityresponse == null) {
			return null;
		} else {
			SchemaListEntity entity = entityresponse.readEntity(SchemaListEntity.class);
			return entity.getSchemas();
		}
	}

	@Override
	public TopicHandlerHttp topicCreate(TopicName topic, int partitioncount, short replicationfactor, Map<String, String> configs) throws PropertiesException {
		TopicHandlerEntity entityin = new TopicHandlerEntity(topic.getName(), partitioncount, replicationfactor, configs);
		Response entityresponse = postRestfulService(getRestEndpoint("/topic/byname", topic.getName()), entityin);
		TopicHandlerEntity entityout = entityresponse.readEntity(TopicHandlerEntity.class);
		TopicMetadata metadata = getTopicMetadata(entityout);
		return new TopicHandlerHttp(topic, metadata );
	}

	@Override
	public TopicHandlerHttp topicCreate(String topic, int partitioncount, short replicationfactor, Map<String, String> configs) throws PropertiesException {
		return topicCreate(new TopicName(topic), partitioncount, replicationfactor, configs);
	}

	private TopicMetadata getTopicMetadata(TopicHandlerEntity entity) {
		TopicMetadata metadata = new TopicMetadata();
		Map<String, String> configs;
		List<ConfigPair> configlist = entity.getConfiglist();
		if (configlist == null) {
			configs = null;
		} else {
			configs = new HashMap<>();
			for (ConfigPair kv : configlist) {
				configs.put(kv.getKey(), kv.getValue());
			}
		}
		metadata.setConfigs(configs);
		metadata.setPartitionCount(entity.getPartitioncount());
		metadata.setReplicationFactor(entity.getReplicationfactor());
		metadata.setPartitions(entity.getPartitionRestEntities());
		return metadata;
	}

	@Override
	public TopicHandlerHttp topicCreate(TopicName topic, int partitioncount, short replicationfactor) throws PropertiesException {
		return topicCreate(topic, partitioncount, replicationfactor, null);
	}

	@Override
	public TopicHandlerHttp topicCreate(String topic, int partitioncount, short replicationfactor) throws PropertiesException {
		return topicCreate(topic, partitioncount, replicationfactor, null);
	}

	@Override
	public synchronized TopicHandlerHttp getTopicOrCreate(String topicname, int partitioncount, short replicationfactor, Map<String, String> configs) throws PropertiesException {
		TopicHandlerHttp t = getTopic(topicname);
		if (t == null) {
			t = topicCreate(new TopicName(topicname), replicationfactor, replicationfactor, configs);
		} 
		return t;
	}

	
	@Override
	public TopicHandlerHttp getTopic(String topicname) throws PropertiesException {
		Response entityresponse = callRestfulservice(getRestEndpoint("/topic/byname", topicname));
		if (entityresponse == null) {
			return null;
		} else {
			TopicHandlerEntity entityout = entityresponse.readEntity(TopicHandlerEntity.class);
			return new TopicHandlerHttp(topicname, getTopicMetadata(entityout));
		}
	}

	@Override
	public TopicHandlerHttp getTopic(TopicName topic) throws PropertiesException {
		return getTopic(topic.getName());
	}

	@Override
	public List<String> getTopics() throws PropertiesException {
		Response entityresponse = callRestfulservice(getRestEndpoint("/topic", "list"));
		if (entityresponse == null) {
			return null;
		} else {
			TopicListEntity entityout = entityresponse.readEntity(TopicListEntity.class);
			return entityout.getTopics();
		}
	}

	@Override
	public List<TopicPayload> getLastRecords(String topicname, int count) throws IOException {
		Response entityresponse = callRestfulservice(getRestEndpoint("/data/preview/count", topicname, String.valueOf(count)));
		if (entityresponse == null) {
			return null;
		} else {
			TopicPayloadBinaryData entityout = entityresponse.readEntity(TopicPayloadBinaryData.class);
			return entityout.asTopicPayloadList(this, schemaidcache);
		}
	}

	@Override
	public List<TopicPayload> getLastRecords(String topicname, long timestamp) throws IOException {
		Response entityresponse = callRestfulservice(getRestEndpoint("/data/preview/time", topicname, String.valueOf(timestamp)));
		if (entityresponse == null) {
			return null;
		} else {
			TopicPayloadBinaryData entityout = entityresponse.readEntity(TopicPayloadBinaryData.class);
			return entityout.asTopicPayloadList(this, schemaidcache);
		}
	}

	@Override
	public List<TopicPayload> getLastRecords(TopicName topicname, int count) throws IOException {
		return getLastRecords(topicname.getName(), count);
	}

	@Override
	public List<TopicPayload> getLastRecords(TopicName topicname, long timestamp) throws IOException {
		return getLastRecords(topicname.getName(), timestamp);
	}

	@Override
	public void open() throws PropertiesException {
        URI uri;
		try {
			uri = new URI(properties.getAdapterServerURI());
		} catch (URISyntaxException e) {
			throw new PropertiesException("The provided URL for the ConnectionServer target is of invalid format", e, null, properties.getAdapterServerURI());
		}
		
		HttpAuthenticationFeature auth = HttpAuthenticationFeature.basic(properties.getUser(), properties.getPassword());
		
		ClientConfig configuration = new ClientConfig();
		configuration.property(ClientProperties.CONNECT_TIMEOUT, 10000);
		configuration.property(ClientProperties.READ_TIMEOUT, 10000);
		Client client = ClientBuilder
				.newBuilder()
				.register(JacksonFeature.class)
				.withConfig(configuration)
				.sslContext(HttpUtil.sslcontext)
				.hostnameVerifier(HttpUtil.verifier)
				.register(auth)
				.build();

		target = client.target(uri);
        try {
        	transactionendpointsend = new URI(getBaseURI().toString() + "/data/producer");
        	transactionendpointfetch = new URI(getBaseURI().toString() + "/data/consumer");
		} catch (URISyntaxException e) {
			throw new PropertiesException("Cannot create the endpoints for send and fetch", e, null);
		}
	}

	@Override
	public void close() {
		target = null;
	}

	@Override
	public void removeProducerMetadata(String producername) throws PropertiesException {
		deleteRestfulService(getRestEndpoint("/meta/producer", producername));
	}

	@Override
	public void removeConsumerMetadata(String consumername) throws PropertiesException {
		deleteRestfulService(getRestEndpoint("/meta/consumer", consumername));
	}

	@Override
	public void addProducerMetadata(ProducerEntity producer) throws PropertiesException {
		postRestfulService(getRestEndpoint("/meta/producer"), producer);
	}

	@Override
	public void addConsumerMetadata(ConsumerEntity consumer) throws PropertiesException {
		postRestfulService(getRestEndpoint("/meta/consumer"), consumer);
	}

	@Override
	public void addServiceMetadata(ServiceEntity consumer) throws PropertiesException {
		postRestfulService(getRestEndpoint("/meta/service"), consumer);
	}

	@Override
	public ProducerMetadataEntity getProducerMetadata() throws PropertiesException {
		Response entityresponse = callRestfulservice(getRestEndpoint("/meta/producer"));
		if (entityresponse == null) {
			return null;
		} else {
			ProducerMetadataEntity entityout = entityresponse.readEntity(ProducerMetadataEntity.class);
			return entityout;
		}
	}

	@Override
	public ConsumerMetadataEntity getConsumerMetadata() throws PropertiesException {
		Response entityresponse = callRestfulservice(getRestEndpoint("/meta/consumer"));
		if (entityresponse == null) {
			return null;
		} else {
			ConsumerMetadataEntity entityout = entityresponse.readEntity(ConsumerMetadataEntity.class);
			return entityout;
		}
	}

	@Override
	public ServiceMetadataEntity getServiceMetadata() throws PropertiesException {
		Response entityresponse = callRestfulservice(getRestEndpoint("/meta/service"));
		if (entityresponse == null) {
			return null;
		} else {
			ServiceMetadataEntity entityout = entityresponse.readEntity(ServiceMetadataEntity.class);
			return entityout;
		}
	}

	@Override
	public ConnectionPropertiesHttp getAPIProperties() {
		return properties;
	}

	protected Response callRestfulservice(String path) throws PipelineRuntimeException {
		checkTarget();
			try {
				Response response = target
						.path(path)
						.request(MediaType.APPLICATION_JSON_TYPE)
						.get();
				return validateRestfulResponse(response, path);
			} catch (ProcessingException e) {
				if (e.getCause() instanceof ConnectException) {
					throw new PipelineTemporaryException("restful call got an error", e.getCause(), null, path);
				} else {
					throw new PipelineRuntimeException("restful call got an error", e, null, path);
				}
			}
	}
	
	private void checkTarget() throws PipelineRuntimeException {
		if (target == null) {
			throw new PipelineRuntimeException("Pipeline not connected with server yet");			
		}
	}
	
	private Response validateRestfulResponse(Response response, String path) throws PipelineTemporaryException {
		if (response.getStatus() >= 200 && response.getStatus() < 300) {
			return response;
		} else {
			JAXBErrorMessage o = response.readEntity(JAXBErrorMessage.class);
			String t = o.getMessage();
			throw new PipelineTemporaryException("restful call did return status \"" + response.getStatus() + "\"", null, t, path);
		}
	}

	protected Response postRestfulService(String path, Object jsonPOJO) throws PipelineRuntimeException {
		try {
			Response response = target
					.path(path)
					.request(MediaType.APPLICATION_JSON_TYPE)
					.post(Entity.entity(jsonPOJO, MediaType.APPLICATION_JSON_TYPE));
			return validateRestfulResponse(response, path);
		} catch (ProcessingException e) {
			if (e.getCause() instanceof ConnectException) {
				throw new PipelineTemporaryException("restful call got an error", e.getCause(), null, path);
			} else {
				throw new PipelineRuntimeException("restful call got an error", e, null, path);
			}
		}
	}

	protected Response deleteRestfulService(String path) throws PipelineRuntimeException {
		try {
			Response response = target
					.path(path)
					.request(MediaType.APPLICATION_JSON_TYPE)
					.delete();
			return validateRestfulResponse(response, path);
		} catch (ProcessingException e) {
			if (e.getCause() instanceof ConnectException) {
				throw new PipelineTemporaryException("restful call got an error", e.getCause(), null, path);
			} else {
				throw new PipelineRuntimeException("restful call got an error", e, null, path);
			}
		}
	}

	public URI getBaseURI() {
		return target.getUri();
	}

	public URI getTransactionEndpointForSend() {
		return transactionendpointsend;
	}
	
	public URI getTransactionEndpointForFetch() {
		return transactionendpointfetch;
	}

	private String getRestEndpoint(String base) throws PropertiesException {
		return "/rest/" + base;
	}

	/**
	 * @param base baseurl with leading slash
	 * @param detail the detail url to append, without leading slash
	 * @return string formatted as /<tenantid>/<base>/<detail>
	 * @throws PropertiesException 
	 */
	private String getRestEndpoint(String base, String detail) throws PropertiesException {
		return "/rest/" + base + "/" + detail;
	}

	private String getRestEndpoint(String base, String detail1, String detail2) throws PropertiesException {
		return "/rest/" + base + "/" + detail1 + "/" + detail2;
	}

	@Override
	public void loadConnectionProperties() throws PropertiesException {
		properties.read(webinfdir);
	}

	@Override
	public void writeConnectionProperties() throws PropertiesException {
		if (webinfdir == null) {
			throw new PropertiesException("The method loadConnectionProperties() has not been called yet, hence don't know the location of the properties file");
		}
		properties.write(webinfdir);
	}

	@Override
	public String getConnectionLabel() {
		return properties.getAdapterServerURI();
	}

	@Override
	public String getHostName() {
		return IOUtils.getHostname();
	}

	@Override
	public void reloadConnectionProperties() throws PropertiesException {
		properties.read(webinfdir);
	}

	@Override
	public void setWEBINFDir(File webinfdir) {
		this.webinfdir = webinfdir;
	}

	@Override
	public ServiceSession createNewServiceSession(ServiceProperties<?> properties) throws PropertiesException {
		return null;
	}

	@Override
	public void validate() throws IOException {
	}

	@Override
	public boolean isAlive() {
		return true;
	}

	@Override
	public SchemaHandler getOrCreateSchema(SchemaName name, String description, Schema keyschema, Schema valueschema) throws PropertiesException {
		return registerSchema(name, description, keyschema, valueschema);
	}

	@Override
	public void removeServiceMetadata(String consumername) throws IOException {
	}

	@Override
	public String getBackingServerConnectionLabel() {
		return null;
	}

	@Override
	public void setConnectionProperties(ConnectionPropertiesHttp props) {
		this.properties = props;
	}

	@Override
	protected ProducerSessionHttp createProducerSession(ProducerProperties properties) throws PropertiesException {
		return new ProducerSessionHttp(properties, this);
	}

	@Override
	protected ConsumerSessionHttp createConsumerSession(ConsumerProperties properties) throws PropertiesException {
		return new ConsumerSessionHttp(properties, this);
	}

	@Override
	public String getAPIName() {
		return ConnectionPropertiesHttp.APINAME;
	}
}
