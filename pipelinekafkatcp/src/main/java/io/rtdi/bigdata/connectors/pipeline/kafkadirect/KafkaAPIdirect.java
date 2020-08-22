package io.rtdi.bigdata.connectors.pipeline.kafkadirect;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.jackson.JacksonFeature;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.rtdi.bigdata.connector.pipeline.foundation.AvroDeserialize;
import io.rtdi.bigdata.connector.pipeline.foundation.AvroSerializer;
import io.rtdi.bigdata.connector.pipeline.foundation.PipelineAbstract;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaRegistryName;
import io.rtdi.bigdata.connector.pipeline.foundation.ServiceSession;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicPayload;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerMetadataEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.LoadInfo;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ProducerMetadataEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ServiceEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ServiceMetadataEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.TopicEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineTemporaryException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.metadata.subelements.TopicMetadata;
import io.rtdi.bigdata.connector.pipeline.foundation.metadata.subelements.TopicMetadataPartition;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.IOUtils;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;
import io.rtdi.bigdata.connector.properties.ServiceProperties;
import io.rtdi.bigdata.connectors.pipeline.kafkadirect.schemaentity.Converter;
import io.rtdi.bigdata.connectors.pipeline.kafkadirect.schemaentity.SchemaIdResponse;
import io.rtdi.bigdata.connectors.pipeline.kafkadirect.schemaentity.SchemaKey;
import io.rtdi.bigdata.connectors.pipeline.kafkadirect.schemaentity.SchemaRegistryKey;
import io.rtdi.bigdata.connectors.pipeline.kafkadirect.schemaentity.SchemaValue;

public class KafkaAPIdirect extends PipelineAbstract<KafkaConnectionProperties, TopicHandler, ProducerSessionKafkaDirect, ConsumerSessionKafkaDirect> {
	private static final String SERVICE_METADATA = "ServiceMetadata";
	private static final String CONSUMER_METADATA = "ConsumerMetadata";
	private static final String PRODUCER_METADATA = "ProducerMetadata";
	private static final String PRODUCER_TRANSACTIONS = "ProducerTransactions";
	private static final String SCHEMA_TOPIC_NAME = "_schemaregistry";
	private static final String PRODUCER_TRANSACTION_TOPIC_NAME = "_producertransactions";
	public static final String AVRO_FIELD_SOURCE_TRANSACTION_IDENTIFIER = "SourceTransactionIdentifier";
	public static final String AVRO_FIELD_CONNECTION_NAME = "Connectionname";
	public static final String AVRO_FIELD_TOPICNAME = "TopicName";
	public static final String AVRO_FIELD_SCHEMANAME = "SchemaName";
	public static final String AVRO_FIELD_CONSUMERNAME = "ConsumerName";
	public static final String AVRO_FIELD_LASTCHANGED = "LastChanged";
	private static final String AVRO_FIELD_HOSTNAME = "HostName";
	private static final String AVRO_FIELD_APILABEL = "APILabel";
	private static final String AVRO_FIELD_REMOTECONNECTION = "RemoteConnection";
	public static final String AVRO_FIELD_SERVICENAME = "ServiceName";
	public static final String AVRO_FIELD_CONSUMINGTOPICNAME = "ConsumingTopicName";
	public static final String AVRO_FIELD_PRODUCINGTOPICNAME = "ProducingTopicName";
	public static final String AVRO_FIELD_ROW_COUNT = "RowCount";
	public static final String AVRO_FIELD_WAS_SUCCESSFUL = "Successful";
	private static final long METADATAAGE = 6*3600*1000;
		
	private KafkaProducer<byte[], byte[]> producer;
	private AdminClient admin;
	private WebTarget target;

	
	private Cache<Integer, Schema> schemaidcache = Caffeine.newBuilder().expireAfterAccess(Duration.ofMinutes(30)).maximumSize(1000).build();
	private Cache<SchemaRegistryName, SchemaHandler> schemacache = Caffeine.newBuilder().expireAfterAccess(Duration.ofMinutes(31)).maximumSize(1000).build();
	

    private Map<String, Object> consumerprops = new HashMap<>(); // These are used for admin tasks only, not to read data
    
	private SchemaHandler producermetadataschema;
	private SchemaHandler consumermetadataschema;
	private SchemaHandler servicemetadataschema;
	private SchemaHandler producertransactionschema;
	
	private static Schema producerkeyschema = SchemaBuilder.builder()
			.record("ProducerMetadataKey")
			.fields()
			.requiredString(PipelineAbstract.AVRO_FIELD_PRODUCERNAME)
			.endRecord();
	private static Schema producervalueschema = SchemaBuilder.builder()
			.record("ProducerMetadataValue")
			.fields()
			.requiredString(PipelineAbstract.AVRO_FIELD_PRODUCERNAME)
			.requiredString(AVRO_FIELD_HOSTNAME)
			.requiredString(AVRO_FIELD_APILABEL)
			.requiredString(AVRO_FIELD_REMOTECONNECTION)
			.requiredLong(AVRO_FIELD_LASTCHANGED)
			.name(AVRO_FIELD_TOPICNAME).type().array().items()
				.record("TopicSchema")
				.fields()
				.requiredString(AVRO_FIELD_TOPICNAME)
				.name(AVRO_FIELD_SCHEMANAME).type().array().items().stringType().noDefault()
				.endRecord().noDefault()
			.endRecord();
	private static Schema consumerkeyschema = SchemaBuilder.builder()
			.record("ConsumerMetadataKey")
			.fields()
			.requiredString(AVRO_FIELD_CONSUMERNAME)
			.endRecord();
	private static Schema consumervalueschema = SchemaBuilder.builder()
			.record("ConsumerMetadataValue")
			.fields()
			.requiredString(AVRO_FIELD_CONSUMERNAME)
			.requiredString(AVRO_FIELD_HOSTNAME)
			.requiredString(AVRO_FIELD_APILABEL)
			.optionalString(AVRO_FIELD_REMOTECONNECTION)
			.requiredLong(AVRO_FIELD_LASTCHANGED)
			.name(AVRO_FIELD_TOPICNAME).type().array().items().stringType().noDefault()
			.endRecord();
	private static Schema servicekeyschema = SchemaBuilder.builder()
			.record("ServiceMetadataKey")
			.fields()
			.requiredString(AVRO_FIELD_SERVICENAME)
			.endRecord();
	private static Schema servicevalueschema = SchemaBuilder.builder()
			.record("ServiceMetadataValue")
			.fields()
			.requiredString(AVRO_FIELD_SERVICENAME)
			.requiredString(AVRO_FIELD_HOSTNAME)
			.requiredString(AVRO_FIELD_APILABEL)
			.requiredLong(AVRO_FIELD_LASTCHANGED)
			.name(AVRO_FIELD_CONSUMINGTOPICNAME).type().unionOf()
				.nullType()
				.and()
				.array().items().stringType()
				.endUnion().nullDefault()
			.name(AVRO_FIELD_PRODUCINGTOPICNAME).type().unionOf()
				.nullType()
				.and()
				.array().items()
					.record("TopicSchema")
					.fields()
					.requiredString(AVRO_FIELD_TOPICNAME)
					.name(AVRO_FIELD_SCHEMANAME).type().array().items().stringType().noDefault()
					.endRecord()
				.endUnion().nullDefault()
			.endRecord();
	
	private static Schema producertransactionkeyschema = SchemaBuilder.builder()
			.record("ProducerTransactionKey")
			.fields()
			.requiredString(PipelineAbstract.AVRO_FIELD_PRODUCERNAME)
			.requiredString(AVRO_FIELD_SCHEMANAME)
			.requiredInt(PipelineAbstract.AVRO_FIELD_PRODUCER_INSTANCE_NO)
			.endRecord();
	private static Schema producertransactionvalueschema = SchemaBuilder.builder()
			.record("ProducerTransactionValue")
			.fields()
			.requiredString(PipelineAbstract.AVRO_FIELD_PRODUCERNAME)
			.requiredString(AVRO_FIELD_SCHEMANAME)
			.requiredInt(PipelineAbstract.AVRO_FIELD_PRODUCER_INSTANCE_NO)
			.requiredString(AVRO_FIELD_SOURCE_TRANSACTION_IDENTIFIER)
			.requiredLong(AVRO_FIELD_LASTCHANGED)
			.optionalLong(AVRO_FIELD_ROW_COUNT)
			.requiredBoolean(AVRO_FIELD_WAS_SUCCESSFUL)
			.endRecord();

	
	private TopicPartition schemaregistrypartition = new TopicPartition(SCHEMA_TOPIC_NAME, 0);
	private Collection<TopicPartition> schemaregistrypartitions = Collections.singletonList(schemaregistrypartition);
	private KafkaConnectionProperties connectionprops = new KafkaConnectionProperties();
	private TopicName producermetadatatopicname;
	private TopicName consumermetadatatopicname;
	private TopicName servicemetadatatopicname;
	private TopicName producertransactiontopicname;

	public KafkaAPIdirect(File rootdir) throws PropertiesException {
		this();
		setConnectionProperties(new KafkaConnectionProperties(rootdir));
	}
    
	public KafkaAPIdirect() {
		super();
		consumerprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		consumerprops.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10000);
		consumerprops.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
		
		consumerprops.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		consumerprops.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
	}
	
    public KafkaAPIdirect(KafkaConnectionProperties kafkaconnectionproperties) throws PropertiesException {
    	this();
    	setConnectionProperties(kafkaconnectionproperties);
    }


	@Override
	protected ProducerSessionKafkaDirect createProducerSession(ProducerProperties properties) throws PropertiesException {
		return new ProducerSessionKafkaDirect(properties, this);
	}

	@Override
	protected ConsumerSessionKafkaDirect createConsumerSession(ConsumerProperties properties) throws PropertiesException {
		return new ConsumerSessionKafkaDirect(properties, this);
	}
		
	@Override
	public String getConnectionLabel() {
		return getAPIProperties().getKafkaBootstrapServers();
	}

	@Override
	public ServiceSession createNewServiceSession(ServiceProperties properties) throws PropertiesException {
		return new ServiceSessionKafkaDirect(properties, this);
	}

	public void setConnectionProperties(KafkaConnectionProperties kafkaconnectionproperties) {
		this.connectionprops = kafkaconnectionproperties;
		consumerprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaconnectionproperties.getKafkaBootstrapServers());
		addSecurityProperties(consumerprops);
	}
    
    void addSecurityProperties(Map<String, Object> propmap) {
		if (connectionprops.getKafkaAPIKey() != null && connectionprops.getKafkaAPIKey().length() > 0) {
			/*
			 * 	security.protocol=SASL_SSL
			 *	sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule   required username="{{ CLUSTER_API_KEY }}"   password="{{ CLUSTER_API_SECRET }}";
			 *	ssl.endpoint.identification.algorithm=https
			 *	sasl.mechanism=PLAIN
			 */
			propmap.put("security.protocol", "SASL_SSL");
			propmap.put(SaslConfigs.SASL_JAAS_CONFIG, 
					"org.apache.kafka.common.security.plain.PlainLoginModule   required username=\"" +
							connectionprops.getKafkaAPIKey() + "\"   password=\"" + 
							connectionprops.getKafkaAPISecret() + "\";");
			propmap.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM);
			propmap.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
		}    	
    }
    
	@Override
    public void open() throws PropertiesException {
    	if (producer == null) {
    		try {
		        Map<String, Object> producerprops = new HashMap<>();
				producerprops.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionprops.getKafkaBootstrapServers());
				producerprops.put(ProducerConfig.ACKS_CONFIG, "all");
				producerprops.put(ProducerConfig.RETRIES_CONFIG, 0);
				producerprops.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
				producerprops.put(ProducerConfig.LINGER_MS_CONFIG, 1);
				producerprops.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
				producerprops.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
				producerprops.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
				addSecurityProperties(producerprops);

				Map<String, Object> adminprops = new HashMap<>();
				adminprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionprops.getKafkaBootstrapServers());
				addSecurityProperties(adminprops);
		
				producer = new KafkaProducer<byte[], byte[]>(producerprops);
				admin = AdminClient.create(adminprops);
				
				if (getAPIProperties().getKafkaSchemaRegistry() != null) {
					URI uri;
					try {
						uri = new URI(getAPIProperties().getKafkaSchemaRegistry());
					} catch (URISyntaxException e) {
						throw new PropertiesException("The provided URL for the SchemaRegistry is of invalid format", e, null, getAPIProperties().getKafkaSchemaRegistry());
					}
					
					ClientConfig configuration = new ClientConfig();
					configuration.property(ClientProperties.CONNECT_TIMEOUT, 10000);
					configuration.property(ClientProperties.READ_TIMEOUT, 10000);
					Client client = ClientBuilder.newClient(configuration).register(JacksonFeature.class);
					if (getAPIProperties().getKafkaSchemaRegistryKey() != null && getAPIProperties().getKafkaSchemaRegistryKey().length() > 0) {
						HttpAuthenticationFeature auth = HttpAuthenticationFeature.basic(
								getAPIProperties().getKafkaSchemaRegistryKey(), 
								getAPIProperties().getKafkaSchemaRegistrySecret());
						client.register(auth);
					}
	
					target = client.target(uri);
				} else {
					target = null;
					createInternalTopic(TopicName.create(SCHEMA_TOPIC_NAME));					
				}

								
				SchemaRegistryName producermetadataschemaname = SchemaRegistryName.create(PRODUCER_METADATA);
				producermetadataschema = getOrCreateSchema(producermetadataschemaname, null, producerkeyschema, producervalueschema);
				
				SchemaRegistryName consumermetadataschemaname = SchemaRegistryName.create(CONSUMER_METADATA);
				consumermetadataschema = getOrCreateSchema(consumermetadataschemaname, null, consumerkeyschema, consumervalueschema);
				
				SchemaRegistryName servicemetadataschemaname = SchemaRegistryName.create(SERVICE_METADATA);
				servicemetadataschema = getOrCreateSchema(servicemetadataschemaname, null, servicekeyschema, servicevalueschema);

				SchemaRegistryName producertransactionname = SchemaRegistryName.create(PRODUCER_TRANSACTIONS);
				producertransactionschema = getOrCreateSchema(producertransactionname, null, producertransactionkeyschema, producertransactionvalueschema);

				producertransactiontopicname = TopicName.create(PRODUCER_TRANSACTION_TOPIC_NAME);
				createInternalTopic(producertransactiontopicname);
				
				producermetadatatopicname = TopicName.create(PRODUCER_METADATA);
				createInternalTopic(producermetadatatopicname);
				consumermetadatatopicname = TopicName.create(CONSUMER_METADATA);
				createInternalTopic(consumermetadatatopicname);
				servicemetadatatopicname = TopicName.create(SERVICE_METADATA);
				createInternalTopic(servicemetadatatopicname);
				
    		} catch (PipelineRuntimeException e) {
				close();
				throw e;
    		} catch (KafkaException e) {
				close();
    			throw new PipelineRuntimeException("KafkaException", e, null);
    		}
    	}
    }

    @Override
	public Schema getSchema(int schemaid) throws PropertiesException {
    	Schema schema = schemaidcache.getIfPresent(schemaid);
    	if (schema != null) {
    		return schema;
    	} else {
    		if (target == null) {
				try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerprops);) {
					consumer.assign(schemaregistrypartitions);
					consumer.seekToBeginning(schemaregistrypartitions);
		
					Map<TopicPartition, Long> lastoffsetmap = consumer.endOffsets(schemaregistrypartitions);
					long lastoffset = lastoffsetmap.get(schemaregistrypartition)-1;
					long lastreadoffset = 0; // Important, else in case the schema topic is empty, it would never return
					do {
						ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(5000));
						Iterator<ConsumerRecord<byte[], byte[]>> recordsiterator = records.iterator();
						while (recordsiterator.hasNext()) {
							ConsumerRecord<byte[], byte[]> record = recordsiterator.next();
							SchemaRegistryKey key = Converter.getKey(record.key());
							if (key != null && key instanceof SchemaKey && record.value() != null) {
								SchemaValue schemadef = Converter.getSchema(record.value());
								if (schemadef.getId() == schemaid) {
									schema = new Schema.Parser().parse(schemadef.getSchema());
									schemaidcache.put(schemaid, schema);
									return schema;
								}
							}
							lastreadoffset = record.offset();
						}
					} while (lastreadoffset < lastoffset);
			    	return null;
				} catch (IOException e) {
					throw new PipelineRuntimeException("Reading the schema from the server failed", e, null);
				}
    		} else {
    			Response entityresponse = callRestfulservice("/schemas/ids/" + String.valueOf(schemaid));
    			if (entityresponse != null) {
	    			SchemaIdResponse entityout = entityresponse.readEntity(SchemaIdResponse.class);
	    			schema = new Schema.Parser().parse(entityout.getSchema());
	    			schemaidcache.put(schemaid, schema);
					return schema;
    			} else {
    				return null;
    			}
    		}
    	}
	}
	
	private TopicHandler createInternalTopic(TopicName topicname) throws PropertiesException {
		TopicHandler topichandler = getTopic(topicname);
		if (topichandler == null) {
			HashMap<String, String> props = new HashMap<String, String>();
			props.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
			return topicCreate(topicname, 1, (short) 1, props);
		} else {
			return topichandler;
		}
	}
	
      
    @Override
	public SchemaHandler getSchema(SchemaRegistryName kafkaschemaname) throws PropertiesException {
    	SchemaHandler handler = schemacache.getIfPresent(kafkaschemaname);
    	if (handler == null) {
    		if (target == null) {
    	    	SchemaValue keyschemadef = null;
    	    	SchemaValue valueschemadef = null;
				try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerprops);) {
					consumer.assign(schemaregistrypartitions);
					consumer.seekToBeginning(schemaregistrypartitions);
					Map<TopicPartition, Long> lastoffsetmap = consumer.endOffsets(schemaregistrypartitions);
					long lastoffset = lastoffsetmap.get(schemaregistrypartition)-1;
					long lastreadoffset = 0; // Important, else in case the schema topic is empty, it would never return
					do {
						ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(5000));
						Iterator<ConsumerRecord<byte[], byte[]>> recordsiterator = records.iterator();
						while (recordsiterator.hasNext()) {
							ConsumerRecord<byte[], byte[]> record = recordsiterator.next();
							SchemaRegistryKey key = Converter.getKey(record.key());
							if (key != null && key instanceof SchemaKey && record.value() != null) {
								SchemaValue schemadef = Converter.getSchema(record.value());
								if (!schemadef.isDeleted()) {
									if (schemadef.getSubject().equals(kafkaschemaname.getEncodedName() + "-key")) {
										keyschemadef = schemadef;
									} else if (schemadef.getSubject().equals(kafkaschemaname.getEncodedName() + "-value")) {
										valueschemadef = schemadef;
									}
								}
							}
							lastreadoffset = record.offset();
						}
					} while (lastreadoffset < lastoffset);
					if (keyschemadef != null && valueschemadef != null) {
						Schema keyschema = new Schema.Parser().parse(keyschemadef.getSchema());
						Schema valueschems = new Schema.Parser().parse(valueschemadef.getSchema());
						handler = new SchemaHandler(kafkaschemaname, keyschema, valueschems, keyschemadef.getId(), valueschemadef.getId());
						schemacache.put(kafkaschemaname, handler);
						return handler;
					} else {
						return null;
					}
				} catch (IOException e) {
					throw new PipelineRuntimeException("Reading the schema from the server failed", e, null);
				}
    		} else {
    			Response entityresponse = callRestfulservice("subjects/" + kafkaschemaname.getEncodedName() + "-key/versions/latest");
    			if (entityresponse != null) {
	    			SchemaValue keyschemadef = entityresponse.readEntity(SchemaValue.class);
					Schema keyschema = new Schema.Parser().parse(keyschemadef.getSchema());
					
	    			entityresponse = callRestfulservice("subjects/" + kafkaschemaname.getEncodedName() + "-value/versions/latest");
	    			SchemaValue valueschemadef = entityresponse.readEntity(SchemaValue.class);
	    			Schema valueschems = new Schema.Parser().parse(valueschemadef.getSchema()); // Parser does cache names hence cannot be reused
	    			
					handler = new SchemaHandler(kafkaschemaname, keyschema, valueschems, keyschemadef.getId(), valueschemadef.getId());
					schemacache.put(kafkaschemaname, handler);
					return handler;
    			} else {
    				return null;
    			}
    		}
    	} else {
    		return handler;
    	}
	}

    public SchemaHandler getOrCreateSchema(SchemaRegistryName schemaname, String description, Schema keyschema, Schema valueschema) throws PropertiesException {
		int keyid = -1;
		int valueid = -1;
		SchemaValue keyschemadef = null;
		SchemaValue valueschemadef = null;
		String keysubject = schemaname.toString() + "-key";
		String valuesubject = schemaname.toString() + "-value";
		if (target == null) {
			long lastreadoffset = 0; // Important, else in case the schema topic is empty, it would never return
			try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerprops);) {
				consumer.assign(schemaregistrypartitions);
				consumer.seekToBeginning(schemaregistrypartitions);
				Map<TopicPartition, Long> lastoffsetmap = consumer.endOffsets(schemaregistrypartitions);
				long lastoffset = lastoffsetmap.get(schemaregistrypartition)-1;
				do {
					ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(5000));
					Iterator<ConsumerRecord<byte[], byte[]>> recordsiterator = records.iterator();
					while (recordsiterator.hasNext()) {
						ConsumerRecord<byte[], byte[]> record = recordsiterator.next();
						SchemaRegistryKey key = Converter.getKey(record.key());
						if (key != null && key instanceof SchemaKey && record.value() != null) {
							SchemaValue schemadef = Converter.getSchema(record.value());
							if (!schemadef.isDeleted()) {
								Schema schema = new Schema.Parser().parse(schemadef.getSchema());
								if (schema.equals(keyschema)) {
									keyid = schemadef.getId();
								}
								if (schema.equals(valueschema)) {
									valueid = schemadef.getId();
								}
								if (schemadef.getSubject().equals(keysubject)) {
									keyschemadef = schemadef;
								} else if (schemadef.getSubject().equals(valuesubject)) {
									valueschemadef = schemadef;
								}
							}
						}
						lastreadoffset = record.offset();
					}
				} while (lastreadoffset < lastoffset);
				/*
				 * Now we might have the schema id and the schemadef. They might come from the same or from different.
				 * Case 1: nothing found, brand new.
				 * Case 2: The schema has been created already and nothing changed.
				 * Case 3: The id is known from a different subject, its id value is to be reused and added to a new entry with the subject name 
				 */
				int newkeyid;
				if (keyid == -1) {
					newkeyid = (int) lastreadoffset++;
				} else {
					newkeyid = keyid;
				}
				if (keyschemadef == null) {
					registerSubject(keysubject, newkeyid, 1, keyschema);
				} else if (keyschemadef.getId() != keyid) { // the subject was found but either it is a new schema with a new id or the id is different
					registerSubject(keysubject, newkeyid, keyschemadef.getVersion()+1, keyschema);
				}
	
				int newvalueid;
				if (valueid == -1) {
					newvalueid = (int) lastreadoffset++;
				} else {
					newvalueid = valueid;
				}
				if (valueschemadef == null) {
					registerSubject(valuesubject, newvalueid, 1, valueschema);
				} else if (valueschemadef.getId() != valueid) { // the subject was found but either it is a new schema with a new id or the id is different
					registerSubject(valuesubject, newvalueid, valueschemadef.getVersion()+1, valueschema);
				}
				SchemaHandler schemahandler = new SchemaHandler(schemaname, keyschema, valueschema, newkeyid, newvalueid);
				schemacache.put(schemaname, schemahandler);
				return schemahandler;
			} catch (IOException e) {
				throw new PipelineRuntimeException("Registering the schema in the server failed", e, null);
			}
		} else {
			SchemaIdResponse post = new SchemaIdResponse();
			post.setSchema(keyschema.toString());
			Response entityresponse = postRestfulService("/subjects/" + keysubject + "/versions", post);
			if (entityresponse != null) {
				SchemaValue entitykey = entityresponse.readEntity(SchemaValue.class);
	
				post.setSchema(valueschema.toString());
				entityresponse = postRestfulService("/subjects/" + valuesubject + "/versions", post);
				SchemaValue entityvalue = entityresponse.readEntity(SchemaValue.class);
	
				SchemaHandler schemahandler = new SchemaHandler(schemaname, keyschema, valueschema, entitykey.getId(), entityvalue.getId());
				schemacache.put(schemaname, schemahandler);
				return schemahandler;
			} else {
				throw new PipelineRuntimeException("Registering the schema in the server failed", null, null);
			}
		}
	}
   
   private void registerSubject(String subjectname, int id, int version, Schema schema) throws PipelineRuntimeException {
	   	SchemaValue valuedef = new SchemaValue(subjectname, version, id, schema.toString(), false);
		SchemaKey keydef = new SchemaKey(subjectname, version);
		ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(SCHEMA_TOPIC_NAME, null, Converter.serilalizeKey(keydef), Converter.serilalizeValue(valuedef));
		// add new subject
		Future<RecordMetadata> future = producer.send(record);
		try {
			future.get(30, TimeUnit.SECONDS);
			schemaidcache.put(id, schema);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			throw new PipelineRuntimeException("Subject was not added successfully within 30 seconds", e, null, subjectname);
		}
   }
		
	public TopicHandler topicCreate(TopicName topic, int partitioncount, short replicationfactor,
			Map<String, String> props) throws PropertiesException {
		try {
			Optional<Short> repfactor;
			if (replicationfactor <= 1) {
				repfactor = Optional.empty();
			} else {
				repfactor = Optional.of(replicationfactor);
			}
			NewTopic t = new NewTopic(topic.toString(), Optional.of(partitioncount), repfactor);
			if (props != null) {
				t.configs(props);
			}
			Collection<NewTopic> newTopics = Collections.singleton( t );
			CreateTopicsResult result = admin.createTopics(newTopics);
			result.all().get(10, TimeUnit.SECONDS);
			TopicMetadata topicdetails = new TopicMetadata();
			topicdetails.setPartitionCount(partitioncount);
			topicdetails.setReplicationFactor(replicationfactor);
			TopicHandler topichandler = new TopicHandler(topic, topicdetails);
			return topichandler;
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			throw new PipelineRuntimeException("Creation of the topic failed", e, null, topic.getName());
		}
	}

	
    @Override
	public TopicHandler getTopic(TopicName kafkatopicname) throws PropertiesException {
		try {
			Collection<String> topicNames = Collections.singleton(kafkatopicname.getName());
			DescribeTopicsResult result = admin.describeTopics(topicNames);
			Map<String, TopicDescription> v = result.all().get(10, TimeUnit.SECONDS);
			TopicDescription d = v.get(kafkatopicname.getName());
			TopicMetadata m = new TopicMetadata();
			m.setPartitionCount(d.partitions().size());
			List<TopicMetadataPartition> partitionRestEntities = new ArrayList<TopicMetadataPartition>();
			for (TopicPartitionInfo p : d.partitions()) {
				TopicMetadataPartition rest = new TopicMetadataPartition();
				rest.setPartition(p.partition());
				rest.setLeader(p.leader().id());
				partitionRestEntities.add(rest);
			}
			m.setPartitions(partitionRestEntities);
			return new TopicHandler(kafkatopicname, m);
		} catch (InterruptedException | TimeoutException e) {
			throw new PipelineTemporaryException("Topic cannot be read", e, kafkatopicname.getName());
		} catch (ExecutionException e) {
			if (e.getCause() == null || e.getCause().getClass().equals(UnknownTopicOrPartitionException.class) == false) {
				throw new PipelineRuntimeException("Topic cannot be read", e, kafkatopicname.getName());
			} else { // return null if the topic is not known yet
				return null;
			}
		}
	}

    @Override
	public List<String> getTopics() throws PipelineRuntimeException {
		try {
			if (admin != null) {
				ListTopicsResult result = admin.listTopics();
				Collection<TopicListing> listing = result.listings().get(10, TimeUnit.SECONDS);
				List<String> topicNames = new ArrayList<>();
				for ( TopicListing t : listing) {
					String n = t.name();
					if (!n.equals(PRODUCER_METADATA) && !n.equals(CONSUMER_METADATA) && !n.equals(SERVICE_METADATA)) {
						topicNames.add(n);
					}
				}
				Collections.sort(topicNames);
				return topicNames;
			} else {
				throw new PipelineRuntimeException("Reading the list of topics failed as there is no valid connection with the backend", null, null);
			}
		} catch (InterruptedException | TimeoutException e) {
			throw new PipelineTemporaryException("Reading the list of topics failed", e, null);
		} catch (ExecutionException e) {
			throw new PipelineRuntimeException("Reading the list of topics failed", e, null);
		}
	}

    @Override
	public void close() {
		try {
			if (producer != null) {
				producer.close();
			}
		} catch (KafkaException e) {
		}
		try {
			if (admin != null) {
				admin.close();
			}
		} catch (KafkaException e) {
		}
		producer = null;
		admin = null;
	}

    @Override
    public boolean isAlive() {
    	return admin != null;
    }
    
    @Override
	public List<TopicPayload> getAllRecordsSince(TopicName kafkatopicname, long timestamp, int count, SchemaRegistryName schema) throws PipelineRuntimeException {
		if (kafkatopicname == null) {
			throw new PipelineRuntimeException("No topicname passed into into the preview method");
		} else {
			if (count <= 0) {
				count = 10;
			}
			Map<String, Object> consumerprops = new HashMap<>();
			consumerprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionprops.getKafkaBootstrapServers());
			consumerprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
			consumerprops.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, count);
			consumerprops.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
			consumerprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			addSecurityProperties(consumerprops);
	
			consumerprops.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
			consumerprops.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
			try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(consumerprops);) {
				List<PartitionInfo> partitioninfos = consumer.partitionsFor(kafkatopicname.toString());
				Map<TopicPartition, Long> timestamplist = new HashMap<>();
				Collection<TopicPartition> partitions = new ArrayList<TopicPartition>(partitioninfos.size());
				for (PartitionInfo p : partitioninfos) {
					TopicPartition t = new TopicPartition(kafkatopicname.toString(), p.partition());
					partitions.add(t);
					timestamplist.put(t, timestamp);
				}
				consumer.assign(partitions);
				Map<TopicPartition, OffsetAndTimestamp> startoffsets = consumer.offsetsForTimes(timestamplist, Duration.ofSeconds(20));
				Map<TopicPartition, Long> endoffsetmap = consumer.endOffsets(partitions);
				if (startoffsets != null) { // if null, then no recent data is available
					HashMap<Integer, Long> offsetstoreachtable = new HashMap<Integer, Long>();
					for (TopicPartition partition : partitions) {
						long endoffset = endoffsetmap.get(partition);
						if (endoffset > 0) { // if the offset == 0 then there is no data. Skip reading that partition then
							OffsetAndTimestamp startoffset = startoffsets.get(partition);
							if (startoffset != null) {
								offsetstoreachtable.put(partition.partition(), endoffset-1); // The end offset is the offset of the next one to be produced, hence offset-1
								consumer.seek(partition, new OffsetAndMetadata(startoffset.offset()));
							}
						}
					}
					long maxtimeout = System.currentTimeMillis() + 10000;
					
					ArrayList<TopicPayload> ret = new ArrayList<TopicPayload>();
		
					do {
						ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
						Iterator<ConsumerRecord<byte[], byte[]>> recordsiterator = records.iterator();
						if (recordsiterator.hasNext()) {
							maxtimeout = System.currentTimeMillis() + 10000;
							do {
								ConsumerRecord<byte[], byte[]> record = recordsiterator.next();
								
								long offsettoreach = offsetstoreachtable.get(record.partition());
								if (record.offset() >= offsettoreach) {
									// remove the entry from the offsetstoreachtable when offset was reached
									offsetstoreachtable.remove(record.partition());
								}
				
								JexlRecord keyrecord = null;
								JexlRecord valuerecord = null;
								try {
									keyrecord = AvroDeserialize.deserialize(record.key(), this, schemaidcache);
								} catch (IOException e) {
									throw new PipelineRuntimeException("Cannot deserialize the Avro key record");
								}
								try {
									valuerecord = AvroDeserialize.deserialize(record.value(), this, schemaidcache);
								} catch (IOException e) {
									throw new PipelineRuntimeException("Cannot deserialize the Avro value record");
								}
								if (schema == null || valuerecord.getSchema().getFullName().equals(schema.toString())) {
									TopicPayload data = new TopicPayload(kafkatopicname, record.offset(), record.partition(), record.timestamp(),
											keyrecord, valuerecord, keyrecord.getSchemaId(), valuerecord.getSchemaId());
									ret.add(data);
									count--;
								}
							} while (recordsiterator.hasNext());
						}
					} while (offsetstoreachtable.size() != 0 && maxtimeout > System.currentTimeMillis() && count > 0);
					return ret;
				}
			}
		}
		return null;
	}
	
    @Override
	public List<TopicPayload> getLastRecords(TopicName kafkatopicname, int count) throws PipelineRuntimeException {
		if (kafkatopicname == null) {
			throw new PipelineRuntimeException("No topicname passed into into the preview method");
		} else {
			if (count <= 0 || count > 1000) {
				count = 10;
			}
			Map<String, Object> consumerprops = new HashMap<>();
			consumerprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionprops.getKafkaBootstrapServers());
			consumerprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
			consumerprops.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, count);
			consumerprops.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
			consumerprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			addSecurityProperties(consumerprops);
	
			consumerprops.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
			consumerprops.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
			try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(consumerprops);) {
	
				List<PartitionInfo> partitioninfos = consumer.partitionsFor(kafkatopicname.toString());
				Collection<TopicPartition> partitions = new ArrayList<TopicPartition>(partitioninfos.size());
				for (PartitionInfo p : partitioninfos) {
					partitions.add(new TopicPartition(kafkatopicname.toString(), p.partition()));
				}
				consumer.assign(partitions);
				Map<TopicPartition, Long> offsetmap = consumer.endOffsets(partitions);
				Map<TopicPartition, Long> offsetmap_beginning = consumer.beginningOffsets(partitions);
				HashMap<Integer, Long> offsetstoreachtable = new HashMap<Integer, Long>();
				for (TopicPartition p : offsetmap.keySet()) {
					long offset = offsetmap.get(p);
					if (offset > 0) { // if the offset == 0 then there is no data. Skip reading that partition then
						offsetstoreachtable.put(p.partition(), offset-1); // The end offset is the offset of the next one to be produced, hence offset-1
						long from_offset = offset-(count/partitions.size());
						long beginningoffset = offsetmap_beginning.get(p);
						if (from_offset < beginningoffset) {
							from_offset = beginningoffset;
						}
						consumer.seek(p, from_offset);
					}
				}
				
				// An empty topic, that is one where no data is in any partition, can exit immediately with no data
				if (offsetstoreachtable.size() == 0) {
					return null;
				}
				
				long maxtimeout = System.currentTimeMillis() + 10000;
				
				ArrayList<TopicPayload> ret = new ArrayList<TopicPayload>();
	
				do {
					ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
					Iterator<ConsumerRecord<byte[], byte[]>> recordsiterator = records.iterator();
					while (recordsiterator.hasNext()) {
						ConsumerRecord<byte[], byte[]> record = recordsiterator.next();
						
						long offsettoreach = offsetstoreachtable.get(record.partition());
						if (record.offset() >= offsettoreach) {
							// remove the entry from the offsetstoreachtable when offset was reached
							offsetstoreachtable.remove(record.partition());
						}
		
						JexlRecord keyrecord = null;
						JexlRecord valuerecord = null;
						try {
							keyrecord = AvroDeserialize.deserialize(record.key(), this, schemaidcache);
						} catch (IOException e) {
							throw new PipelineRuntimeException("Cannot deserialize the Avro key record");
						}
						try {
							valuerecord = AvroDeserialize.deserialize(record.value(), this, schemaidcache);
						} catch (IOException e) {
							throw new PipelineRuntimeException("Cannot deserialize the Avro value record");
						}
						TopicPayload data = new TopicPayload(kafkatopicname, record.offset(), record.partition(), record.timestamp(),
								keyrecord, valuerecord, keyrecord.getSchemaId(), valuerecord.getSchemaId());
						ret.add(0, data);
					}
				} while (offsetstoreachtable.size() != 0 && maxtimeout > System.currentTimeMillis());
				return ret;
			} 
		}
	}
		
    @Override
	public List<String> getSchemas() throws PipelineRuntimeException {
    	List<String> subjects = new ArrayList<>();
		if (target == null) {
			try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerprops);) {
				consumer.assign(schemaregistrypartitions);
				consumer.seekToBeginning(schemaregistrypartitions);
	
				Map<TopicPartition, Long> lastoffsetmap = consumer.endOffsets(schemaregistrypartitions);
				long lastoffset = lastoffsetmap.get(schemaregistrypartition)-1;
				long lastreadoffset = 0; // Important, else in case the schema topic is empty, it would never return
				do {
					ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(5000));
					Iterator<ConsumerRecord<byte[], byte[]>> recordsiterator = records.iterator();
					while (recordsiterator.hasNext()) {
						ConsumerRecord<byte[], byte[]> record = recordsiterator.next();
						SchemaRegistryKey key = Converter.getKey(record.key());
						if (key != null && key instanceof SchemaKey && record.value() != null) {
							SchemaValue schemadef = Converter.getSchema(record.value());
							if (schemadef.getSubject().endsWith("-key")) {
								String subject = schemadef.getSubject().substring(0, schemadef.getSubject().lastIndexOf('-'));
								if (schemadef.isDeleted()) {
									subjects.remove(subject);
								} else if (!subjects.contains(subject)) {
									subjects.add(subject);
								}
							}
						}
						lastreadoffset = record.offset();
					}
				} while (lastreadoffset < lastoffset);
		    	return null;
			} catch (IOException e) {
				throw new PipelineRuntimeException("Reading the schema from the server failed", e, null);
			}
		} else {
			try {
				Response entityresponse = callRestfulservice("/subjects");
				if (entityresponse != null) {
					List<String> entityout = entityresponse.readEntity(new GenericType<List<String>>() { });
					if (entityout != null) {
						for (String s : entityout) {
							if (s.endsWith("-key")) {
								String subject = s.substring(0, s.lastIndexOf('-'));
								subjects.add(subject);
							}
						}
						return subjects;
					} else {
						return null;
					}
				} else {
					return null;
				}
			} catch (IOException e) {
				throw new PipelineRuntimeException("Reading the schema from the Kafka Schema Registry failed", e, null);
			}
		}
	}
    	
    @Override
	public void removeProducerMetadata(String producername) throws IOException {
    	GenericRecord keyrecord = new Record(producermetadataschema.getKeySchema());
    	keyrecord.put(PipelineAbstract.AVRO_FIELD_PRODUCERNAME, producername);
		byte[] key = AvroSerializer.serialize(producermetadataschema.getDetails().getKeySchemaID(), keyrecord);
    	ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(getTenantProducerMetadataName().getName(), 0, key, null);
    	producer.send(record);
	}

	@Override
	public void removeConsumerMetadata(String consumername) throws IOException {
    	GenericRecord keyrecord = new Record(consumermetadataschema.getKeySchema());
    	keyrecord.put(AVRO_FIELD_CONSUMERNAME, consumername);
		byte[] key = AvroSerializer.serialize(consumermetadataschema.getDetails().getKeySchemaID(), keyrecord);
    	ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(this.getTenantConsumerMetadataName().getName(), 0, key, null);
    	
    	producer.send(record);
	}
	
	@Override
	public void removeServiceMetadata(String servicename) throws IOException {
    	GenericRecord keyrecord = new Record(servicemetadataschema.getKeySchema());
    	keyrecord.put(AVRO_FIELD_SERVICENAME, servicename);
		byte[] key = AvroSerializer.serialize(servicemetadataschema.getDetails().getKeySchemaID(), keyrecord);
    	ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(this.getTenantServiceMetadataName().getName(), 0, key, null);
    	producer.send(record);
	}

	
    @Override
	public void addProducerMetadata(ProducerEntity producer) throws IOException {
    	if (producer != null) {
        	GenericRecord keyrecord = new Record(producermetadataschema.getKeySchema());
        	keyrecord.put(PipelineAbstract.AVRO_FIELD_PRODUCERNAME, producer.getProducerName());
        	GenericRecord valuerecord = new Record(producermetadataschema.getValueSchema());
        	valuerecord.put(PipelineAbstract.AVRO_FIELD_PRODUCERNAME, producer.getProducerName());
        	valuerecord.put(AVRO_FIELD_LASTCHANGED, System.currentTimeMillis());
	    	valuerecord.put(AVRO_FIELD_HOSTNAME, producer.getHostname());
	    	valuerecord.put(AVRO_FIELD_APILABEL, producer.getApiconnection());
	    	valuerecord.put(AVRO_FIELD_REMOTECONNECTION, producer.getRemoteconnection());

	    	List<GenericRecord> topics = new ArrayList<>();
	    	for (TopicEntity t : producer.getTopicList()) {
	    		GenericRecord topicrecord = new Record(producermetadataschema.getValueSchema().getField(AVRO_FIELD_TOPICNAME).schema().getElementType());
	    		topicrecord.put(AVRO_FIELD_TOPICNAME, t.getTopicName());
	    		topicrecord.put(AVRO_FIELD_SCHEMANAME, t.getSchemaList());
	    		topics.add(topicrecord);
	    	}
	    	
	    	valuerecord.put(AVRO_FIELD_TOPICNAME, topics);
	    	
			byte[] key = AvroSerializer.serialize(producermetadataschema.getDetails().getKeySchemaID(), keyrecord);
			byte[] value = AvroSerializer.serialize(producermetadataschema.getDetails().getValueSchemaID(), valuerecord);
	    	
	    	ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(getTenantProducerMetadataName().getName(), 0, key, value);
	    	
	    	this.producer.send(record);

    	}
	}

	@Override
	public void addConsumerMetadata(ConsumerEntity consumer) throws IOException {
		if (consumer != null) {
	    	GenericRecord keyrecord = new Record(consumermetadataschema.getKeySchema());
	    	keyrecord.put(AVRO_FIELD_CONSUMERNAME, consumer.getConsumerName());
	    	GenericRecord valuerecord = new Record(consumermetadataschema.getValueSchema());
	    	valuerecord.put(AVRO_FIELD_CONSUMERNAME, consumer.getConsumerName());
	    	valuerecord.put(AVRO_FIELD_LASTCHANGED, System.currentTimeMillis());
	    	valuerecord.put(AVRO_FIELD_HOSTNAME, consumer.getHostname());
	    	valuerecord.put(AVRO_FIELD_REMOTECONNECTION, consumer.getRemoteconnection());
	    	valuerecord.put(AVRO_FIELD_APILABEL, consumer.getApiconnection());
	    	valuerecord.put(AVRO_FIELD_TOPICNAME, consumer.getTopicList());
	    	
			byte[] key = AvroSerializer.serialize(consumermetadataschema.getDetails().getKeySchemaID(), keyrecord);
			byte[] value = AvroSerializer.serialize(consumermetadataschema.getDetails().getValueSchemaID(), valuerecord);
	    	
	    	ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(getTenantConsumerMetadataName().getName(), 0, key, value);
	    	
	    	producer.send(record);
		}
	}
	
    @Override
	public void addServiceMetadata(ServiceEntity service) throws IOException {
    	if (service != null) {
        	GenericRecord keyrecord = new Record(servicemetadataschema.getKeySchema());
        	keyrecord.put(AVRO_FIELD_SERVICENAME, service.getServiceName());
        	GenericRecord valuerecord = new Record(servicemetadataschema.getValueSchema());
        	valuerecord.put(AVRO_FIELD_SERVICENAME, service.getServiceName());
        	valuerecord.put(AVRO_FIELD_LASTCHANGED, System.currentTimeMillis());
	    	valuerecord.put(AVRO_FIELD_HOSTNAME, service.getHostname());
	    	valuerecord.put(AVRO_FIELD_APILABEL, service.getApiconnection());

	    	valuerecord.put(AVRO_FIELD_CONSUMINGTOPICNAME, service.getConsumedTopicList());

	    	Schema topicschema = IOUtils.getBaseSchema(servicemetadataschema.getValueSchema().getField(AVRO_FIELD_PRODUCINGTOPICNAME).schema()).getElementType();
	    	if (service.getProducedTopicList() != null) {
		    	List<GenericRecord> topics = new ArrayList<>();
		    	for (TopicEntity t : service.getProducedTopicList()) {
		    		GenericRecord topicrecord = new Record(topicschema);
		    		topicrecord.put(AVRO_FIELD_TOPICNAME, t.getTopicName());
		    		topicrecord.put(AVRO_FIELD_SCHEMANAME, t.getSchemaList());
		    		topics.add(topicrecord);
		    	}
		    	
		    	valuerecord.put(AVRO_FIELD_PRODUCINGTOPICNAME, topics);
	    	}
	    	
			byte[] key = AvroSerializer.serialize(servicemetadataschema.getDetails().getKeySchemaID(), keyrecord);
			byte[] value = AvroSerializer.serialize(servicemetadataschema.getDetails().getValueSchemaID(), valuerecord);
	    	
	    	ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(this.getTenantServiceMetadataName().getName(), 0, key, value);
	    	
	    	this.producer.send(record);

    	}
	}


    @Override
	public ProducerMetadataEntity getProducerMetadata() throws PipelineRuntimeException {
    	long metadatamaxage = System.currentTimeMillis() - METADATAAGE;
		List<TopicPayload> records = getLastRecords(getTenantProducerMetadataName(), 10000); // list is sorted descending
		if (records != null && records.size() > 0) {
			List<ProducerEntity> producerlist = new ArrayList<>();
			Set<String> uniqueproducers = new HashSet<>();
			for (TopicPayload producerdata : records) {
				Object o = producerdata.getValueRecord().get(PipelineAbstract.AVRO_FIELD_PRODUCERNAME);
				if (o != null) { // failsafe if this topic contains wrong data
					String producername = o.toString();
					if (!uniqueproducers.contains(producername)) {
						String hostname = producerdata.getValueRecord().get(AVRO_FIELD_HOSTNAME).toString();
						String apilabel = producerdata.getValueRecord().get(AVRO_FIELD_APILABEL).toString();
						String remoteconnection = producerdata.getValueRecord().get(AVRO_FIELD_REMOTECONNECTION).toString();
						Long lastchanged = (Long) producerdata.getValueRecord().get(AVRO_FIELD_LASTCHANGED);
						
						if (lastchanged < metadatamaxage) {
							// do not add records older than
							break;
						} else {
							Map<String, Set<String>> topicmap = new HashMap<>();
							@SuppressWarnings("unchecked")
							List<GenericRecord> topics = (List<GenericRecord>) producerdata.getValueRecord().get(AVRO_FIELD_TOPICNAME);
							for (GenericRecord topicdata : topics) {
								String topicname = topicdata.get(AVRO_FIELD_TOPICNAME).toString();
								HashSet<String> schemaset = new HashSet<>();
								@SuppressWarnings("unchecked")
								List<Utf8> schemas = (List<Utf8>) topicdata.get(AVRO_FIELD_SCHEMANAME);
								if (schemas != null) {
									for (Utf8 schema : schemas) {
										schemaset.add(schema.toString());
									}
								}
								topicmap.put(topicname, schemaset);
							}
							ProducerEntity producerentity = new ProducerEntity(producername, remoteconnection, hostname, apilabel, topicmap);
							producerlist.add(producerentity);
							uniqueproducers.add(producername);
						}
					}
				}
			}
			if (producerlist.size() > 0) {
				return new ProducerMetadataEntity(producerlist);
			} else {
				return null;
			}
		} else {
			return null;
		}
	}

    @Override
	public ConsumerMetadataEntity getConsumerMetadata() throws PipelineRuntimeException {
    	long metadatamaxage = System.currentTimeMillis() - METADATAAGE;
		List<TopicPayload> records = getLastRecords(getTenantConsumerMetadataName(), 10000); // list is sorted descending
		if (records != null && records.size() > 0) {
			List<ConsumerEntity> list = new ArrayList<>();
			Set<String> uniqueconsumers = new HashSet<>();
			for (TopicPayload consumerrecord : records) {
				Object o = consumerrecord.getValueRecord().get(AVRO_FIELD_CONSUMERNAME);
				if (o != null) { // failsafe if this topic contains wrong data
					String consumername = o.toString();
					if (!uniqueconsumers.contains(consumername)) {
						String hostname = consumerrecord.getValueRecord().get(AVRO_FIELD_HOSTNAME).toString();
						String apilabel = consumerrecord.getValueRecord().get(AVRO_FIELD_APILABEL).toString();
						String remoteconnection = consumerrecord.getValueRecord().get(AVRO_FIELD_REMOTECONNECTION).toString();
						Long lastchanged = (Long) consumerrecord.getValueRecord().get(AVRO_FIELD_LASTCHANGED);
		
						if (lastchanged < metadatamaxage) {
							// do not add records older than
							break;
						} else {
							List<String> topiclist = new ArrayList<>();
							@SuppressWarnings("unchecked")
							List<Utf8> topics = (List<Utf8>) consumerrecord.getValueRecord().get(AVRO_FIELD_TOPICNAME);
							if (topics != null) {
								for (Utf8 topicname : topics) {
									topiclist.add(topicname.toString());
								}
							}
							ConsumerEntity consumerentity = new ConsumerEntity(consumername, remoteconnection, hostname, apilabel, topiclist);
							list.add(consumerentity);
							uniqueconsumers.add(consumername);
						}
					}
				}
			}
			if (list.size() > 0) {
				return new ConsumerMetadataEntity(list);
			} else {
				return null;
			}
		} else {
			return null;
		}
	}
    
    @Override
	public ServiceMetadataEntity getServiceMetadata() throws PipelineRuntimeException {
    	long metadatamaxage = System.currentTimeMillis() - METADATAAGE;
		List<TopicPayload> records = getLastRecords(getTenantServiceMetadataName(), 10000); // list is sorted descending
		if (records != null && records.size() > 0) {
			List<ServiceEntity> servicelist = new ArrayList<>();
			Set<String> uniqueservices = new HashSet<>();
			for (TopicPayload servicedata : records) {
				Object o = servicedata.getValueRecord().get(AVRO_FIELD_SERVICENAME);
				if (o != null) {
					String servicename = o.toString();
					if (!uniqueservices.contains(servicename)) {
						String hostname = servicedata.getValueRecord().get(AVRO_FIELD_HOSTNAME).toString();
						String apilabel = servicedata.getValueRecord().get(AVRO_FIELD_APILABEL).toString();
						Long lastchanged = (Long) servicedata.getValueRecord().get(AVRO_FIELD_LASTCHANGED);
						
						if (lastchanged < metadatamaxage) {
							// do not add records older than
							break;
						} else {
							Map<String, Set<String>> producingtopicmap = new HashMap<>();
							@SuppressWarnings("unchecked")
							List<GenericRecord> producingtopics = (List<GenericRecord>) servicedata.getValueRecord().get(AVRO_FIELD_PRODUCINGTOPICNAME);
							if (producingtopics != null) {
								for (GenericRecord topicdata : producingtopics) {
									String topicname = topicdata.get(AVRO_FIELD_TOPICNAME).toString();
									HashSet<String> schemaset = new HashSet<>();
									@SuppressWarnings("unchecked")
									List<Utf8> schemas = (List<Utf8>) topicdata.get(AVRO_FIELD_SCHEMANAME);
									if (schemas != null) {
										for (Utf8 schema : schemas) {
											schemaset.add(schema.toString());
										}
									}
									producingtopicmap.put(topicname, schemaset);
								}
							}
							
							List<String> consumingtopiclist = new ArrayList<>();
							@SuppressWarnings("unchecked")
							List<Utf8> consumingtopics = (List<Utf8>) servicedata.getValueRecord().get(AVRO_FIELD_CONSUMINGTOPICNAME);
							if (consumingtopics != null) {
								for (Utf8 topicname : consumingtopics) {
									consumingtopiclist.add(topicname.toString());
								}
							}
	
							
							ServiceEntity serviceentity = new ServiceEntity(servicename, hostname, apilabel, consumingtopiclist, producingtopicmap);
							servicelist.add(serviceentity);
							uniqueservices.add(servicename);
						}
					}
				}
			}
			if (servicelist.size() > 0) {
				return new ServiceMetadataEntity(servicelist);
			} else {
				return null;
			}
		} else {
			return null;
		}
	}


	private TopicName getTenantConsumerMetadataName() throws PipelineRuntimeException {
		return consumermetadatatopicname;
	}

	private TopicName getTenantProducerMetadataName() throws PipelineRuntimeException {
		return producermetadatatopicname;
	}

	private TopicName getTenantServiceMetadataName() throws PipelineRuntimeException {
		return servicemetadatatopicname;
	}

	public void loadConnectionProperties() throws PropertiesException {
		setConnectionProperties(new KafkaConnectionProperties(webinfdir));
	}

	@Override
	public void writeConnectionProperties() throws PropertiesException {
		this.getAPIProperties().write(webinfdir);
	}

	protected Response callRestfulservice(String path) throws PipelineRuntimeException {
		try {
			Response response = target
					.path(path)
					.request(MediaType.APPLICATION_JSON_TYPE)
					.get();
			if (response.getStatus() >= 200 && response.getStatus() < 300) {
				return response;
			} else if (response.getStatus() == 404) {
				return null;
			} else {
				throw new PipelineRuntimeException("restful call did return status \"" + response.getStatus() + "\"", null, response.getEntity().toString(), path);
			}
		} catch (ProcessingException e) {
			throw new PipelineRuntimeException("restful call got an error", e, null, path);
		}
	}

	protected Response postRestfulService(String path, Object jsonPOJO) throws PipelineRuntimeException {
		try {
			Response response = target
					.path(path)
					.request(MediaType.APPLICATION_JSON_TYPE)
					.post(Entity.entity(jsonPOJO, "application/vnd.schemaregistry.v1+json"));
			if (response.getStatus() >= 200 && response.getStatus() < 300) {
				return response;
			} else if (response.getStatus() == 404) {
				return null;
			} else {
				throw new PipelineRuntimeException("restful call did return status \"" + response.getStatus() + "\"", null, response.getEntity().toString(), path);
			}
		} catch (ProcessingException e) {
			throw new PipelineRuntimeException("restful call got an error", e, null, path);
		}
	}

	Cache<Integer, Schema> getSchemaIdCache() {
		return schemaidcache;
	}

	@Override
	public void validate() throws IOException {
		Map<String, Object> adminprops = new HashMap<>();
		adminprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionprops.getKafkaBootstrapServers());
		addSecurityProperties(adminprops);
		AdminClient client = null;
		try {
			client = AdminClient.create(adminprops);
		} catch (KafkaException e) {
			throw new PipelineRuntimeException("Validation of the session failed with error \"" + e.getMessage() + "\"", e, "Hostname and port correct? Firewall?");
		} finally {
			if (client != null) {
				client.close(Duration.ofSeconds(30));
			}
		}
	}

	@Override
	public SchemaHandler registerSchema(SchemaRegistryName schemaname, String description, Schema keyschema, Schema valueschema) throws PropertiesException {
		return getOrCreateSchema(schemaname, description, keyschema, valueschema);
	}

	@Override
	public KafkaConnectionProperties getAPIProperties() {
		return connectionprops;
	}

	@Override
	public String getBackingServerConnectionLabel() {
		return null;
	}

	@Override
	public String getAPIName() {
		return KafkaConnectionProperties.APINAME;
	}

	/**
	 * @return the schema handler used for the transaction log
	 */
	public SchemaHandler getTransactionLogSchema() {
		return producertransactionschema;
	}
	
	public TopicName getProducerTransactionTopicName() {
		return producertransactiontopicname;
	}

	@Override
	public Map<String, LoadInfo> getLoadInfo(String producername, int instanceno) throws PipelineRuntimeException {
		/*
		 * data contains all records, oldest is first
		 */
		List<TopicPayload> data = getAllRecordsSince(producertransactiontopicname, 0L, Integer.MAX_VALUE, null);
		Map<String, LoadInfo> r = new HashMap<>();
		for (TopicPayload d : data) {
			JexlRecord record = d.getValueRecord();
			String currentproducername = (String) record.get(PipelineAbstract.AVRO_FIELD_PRODUCERNAME);
			Integer currentinstanceno = (Integer) record.get(PipelineAbstract.AVRO_FIELD_PRODUCER_INSTANCE_NO);
			if (currentproducername.equals(producername) && currentinstanceno == instanceno) {
				Boolean successful = (Boolean)record.get(AVRO_FIELD_WAS_SUCCESSFUL);
				String schemaname = (String) record.get(AVRO_FIELD_SCHEMANAME);
				/*
				 * We only care about the most recent entry, that is the first
				 */
				if (successful) {
					LoadInfo i = new LoadInfo();
					i.setProducername(currentproducername);
					i.setProducerinstanceno(currentinstanceno);
					i.setSchemaname(schemaname);
					i.setTransactionid((String) record.get(AVRO_FIELD_SOURCE_TRANSACTION_IDENTIFIER));
					i.setCompletiontime((Long) record.get(AVRO_FIELD_LASTCHANGED));
					i.setRowcount((Long) record.get(AVRO_FIELD_ROW_COUNT));
					r.put(schemaname, i);
				} else if (r.containsKey(schemaname)) {
					r.remove(schemaname);
				}
			}
		}
		return r;
	}

}
