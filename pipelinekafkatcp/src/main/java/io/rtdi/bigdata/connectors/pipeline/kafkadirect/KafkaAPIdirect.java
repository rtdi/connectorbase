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
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
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
import org.apache.logging.log4j.Logger;
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
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineTemporaryException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.metadata.subelements.TopicMetadata;
import io.rtdi.bigdata.connector.pipeline.foundation.metadata.subelements.TopicMetadataPartition;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.GlobalSettings;
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
	public static final String AVRO_FIELD_OFFSETTABLE = "PartitionOffsets";
	public static final String AVRO_FIELD_PARTITON = "Partition";
	public static final String AVRO_FIELD_PARTITON_OFFSET = "Offset";
	private static final String AVRO_FIELD_TOPICOFFSETTABLE = "TopicOffsets";
		
	private KafkaProducer<byte[], byte[]> producer;
	private AdminClient admin;
	private WebTarget target;
	private Set<String> internaltopics;
	private Set<String> internalschemas;
	
	private Cache<Integer, Schema> schemaidcache = Caffeine.newBuilder().expireAfterWrite(Duration.ofMinutes(30)).maximumSize(1000).build();
	private Cache<SchemaRegistryName, SchemaHandler> schemacache = Caffeine.newBuilder().expireAfterWrite(Duration.ofMinutes(31)).maximumSize(1000).build();
	private LoadInfoContainer loadinfocache = new LoadInfoContainer();

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
			.name(AVRO_FIELD_TOPICOFFSETTABLE).type().unionOf()
				.nullType()
				.and()
				.array().items()
				.record("TopicOffsets")
				.fields()
				.requiredString(AVRO_FIELD_TOPICNAME)
				.name(AVRO_FIELD_OFFSETTABLE).type().unionOf()
					.nullType()
					.and()
					.array().items()
					.record("Offsets")
					.fields()
					.requiredInt(AVRO_FIELD_PARTITON)
					.requiredLong(AVRO_FIELD_PARTITON_OFFSET)
					.endRecord()
				.endUnion().nullDefault()
				.endRecord()
				.endUnion().nullDefault()
			.endRecord();

	
	private TopicPartition schemaregistrypartition;
	private Collection<TopicPartition> schemaregistrypartitions;
	private KafkaConnectionProperties connectionprops = new KafkaConnectionProperties();

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
		addCustomProperties(consumerprops, getGlobalSettings().getKafkaConsumerProperties(), logger);
	}
    
    void addSecurityProperties(Map<String, Object> propmap) {
		if (connectionprops.getKafkaAPIKey() != null && connectionprops.getKafkaAPIKey().length() > 0) {
			if (connectionprops.getKafkaSASLMechanism() == null || connectionprops.getKafkaSASLMechanism().equals(KafkaConnectionProperties.KAFKASASLMECHANISM_PLAIN)) {
				/*
				 * 	security.protocol=SASL_SSL
				 *	sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule \
				 *  	required username="{{ CLUSTER_API_KEY }}"   password="{{ CLUSTER_API_SECRET }}";
				 *	ssl.endpoint.identification.algorithm=https
				 *	sasl.mechanism=PLAIN
				 */
				propmap.put("security.protocol", "SASL_SSL");
				propmap.put(SaslConfigs.SASL_JAAS_CONFIG, 
						"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" +
								connectionprops.getKafkaAPIKey() + "\" password=\"" + 
								connectionprops.getKafkaAPISecret() + "\";");
				propmap.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM);
				propmap.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
			} else {
				/*
				 * security.protocol=SASL_SSL
				 * sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule \
  				 * 		required username="{{ CLUSTER_API_KEY }}"   password="{{ CLUSTER_API_SECRET }}";
				 *	ssl.endpoint.identification.algorithm=https
				 * sasl.mechanism=SCRAM-SHA-256
				 */
				propmap.put("security.protocol", "SASL_SSL");
				propmap.put(SaslConfigs.SASL_JAAS_CONFIG, 
						"org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" +
								connectionprops.getKafkaAPIKey() + "\" password=\"" + 
								connectionprops.getKafkaAPISecret() + "\";");
				propmap.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, SslConfigs.DEFAULT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM);
				propmap.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
			}
			addCustomProperties(propmap, settings.getKafkaConnectionProperties(), logger);
		}    	
    }
    
	@Override
    public void open() throws PropertiesException {
    	if (producer == null) {
    		try {
    			
    			schemaregistrypartition = new TopicPartition(getSchemaRegistryTopicName().getEncodedName(), 0);
    			schemaregistrypartitions = Collections.singletonList(schemaregistrypartition);

		        Map<String, Object> producerprops = new HashMap<>();
				producerprops.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionprops.getKafkaBootstrapServers());
				producerprops.put(ProducerConfig.ACKS_CONFIG, "all");
				producerprops.put(ProducerConfig.RETRIES_CONFIG, 0);
				producerprops.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
				producerprops.put(ProducerConfig.LINGER_MS_CONFIG, 1);
				producerprops.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
				producerprops.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
				producerprops.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
				producerprops.put(ProducerConfig.CLIENT_ID_CONFIG, "MetadataProducer_" + Thread.currentThread().getId());
				addSecurityProperties(producerprops);
				addCustomProperties(producerprops, getGlobalSettings().getKafkaProducerProperties(), logger);

				Map<String, Object> adminprops = new HashMap<>();
				adminprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connectionprops.getKafkaBootstrapServers());
				adminprops.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaAPIAdminClient_" + Thread.currentThread().getId());
				addSecurityProperties(adminprops);
				addCustomProperties(adminprops, getGlobalSettings().getKafkaAdminProperties(), logger);

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
					createInternalTopic(getSchemaRegistryTopicName());					
				}

								
				producermetadataschema = getOrCreateSchema(getProducerMetadataSchemaName(), null, producerkeyschema, producervalueschema);
				consumermetadataschema = getOrCreateSchema(getConsumerMetadataSchemaName(), null, consumerkeyschema, consumervalueschema);
				servicemetadataschema = getOrCreateSchema(getServiceMetadataSchemaName(), null, servicekeyschema, servicevalueschema);
				producertransactionschema = getOrCreateSchema(getTransactionsSchemaName(), null, producertransactionkeyschema, producertransactionvalueschema);

				createInternalTopic(getTransactionsTopicName());
				createInternalTopic(getProducerMetadataTopicName());
				createInternalTopic(getConsumerMetadataTopicName());
				createInternalTopic(getServiceMetadataTopicName());
				
				internaltopics = new HashSet<>();
				internaltopics.add(getTransactionsTopicName().getEncodedName());
				internaltopics.add(getProducerMetadataTopicName().getEncodedName());
				internaltopics.add(getConsumerMetadataTopicName().getEncodedName());
				internaltopics.add(getServiceMetadataTopicName().getEncodedName());
				internaltopics.add(getSchemaRegistryTopicName().getEncodedName());
				
				internalschemas = new HashSet<>();
				internalschemas.add(getProducerMetadataSchemaName().getEncodedName());
				internalschemas.add(getConsumerMetadataSchemaName().getEncodedName());
				internalschemas.add(getServiceMetadataSchemaName().getEncodedName());
				internalschemas.add(getTransactionsSchemaName().getEncodedName());
				
				
    		} catch (PipelineRuntimeException e) {
				close();
				throw e;
    		} catch (KafkaException e) {
				close();
    			throw new PipelineRuntimeException("KafkaException", e, null);
    		}
    	}
    }

    static void addCustomProperties(Map<String, Object> propmap, Properties customprops, Logger logger) {
		if (customprops != null) {
			for (Entry<Object, Object> e : customprops.entrySet()) {
				boolean exists = propmap.containsKey(e.getKey().toString());
				propmap.put(e.getKey().toString(), e.getValue());
				if (exists) {
					logger.info("Overwriting property {} with value {}", e.getKey().toString(), e.getValue().toString());
				} else {
					logger.info("Adding property {} with value {}", e.getKey().toString(), e.getValue().toString());
				}
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
    			Response entityresponse = callRestfulservice(getSubjectsPath() + "/" + kafkaschemaname.getEncodedName() + "-key/versions/latest");
    			if (entityresponse != null) {
	    			SchemaValue keyschemadef = entityresponse.readEntity(SchemaValue.class);
					Schema keyschema = new Schema.Parser().parse(keyschemadef.getSchema());
					
	    			entityresponse = callRestfulservice(getSubjectsPath() + "/" + kafkaschemaname.getEncodedName() + "-value/versions/latest");
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
			Response entityresponse = postRestfulService(getSubjectsPath() + "/" + keysubject + "/versions", post);
			if (entityresponse != null) {
				SchemaValue entitykey = entityresponse.readEntity(SchemaValue.class);
	
				post.setSchema(valueschema.toString());
				entityresponse = postRestfulService(getSubjectsPath() + "/" + valuesubject + "/versions", post);
				SchemaValue entityvalue = entityresponse.readEntity(SchemaValue.class);
	
				SchemaHandler schemahandler = new SchemaHandler(schemaname, keyschema, valueschema, entitykey.getId(), entityvalue.getId());
				schemacache.put(schemaname, schemahandler);
				return schemahandler;
			} else {
				throw new PipelineRuntimeException("Registering the schema in the server failed", null, null);
			}
		}
	}
   
   private String getSubjectsPath() {
		if (settings != null && settings.getSubjectPath() != null) {
			return settings.getSubjectPath();
		} else {
			return "/subjects";
		}
   }

   private void registerSubject(String subjectname, int id, int version, Schema schema) throws PipelineRuntimeException {
	   	SchemaValue valuedef = new SchemaValue(subjectname, version, id, schema.toString(), false);
		SchemaKey keydef = new SchemaKey(subjectname, version);
		ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(
				getSchemaRegistryTopicName().getEncodedName(),
				null,
				Converter.serilalizeKey(keydef),
				Converter.serilalizeValue(valuedef));
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
	public List<TopicName> getTopics() throws PipelineRuntimeException {
		try {
			if (admin != null) {
				ListTopicsResult result = admin.listTopics();
				Collection<TopicListing> listing = result.listings().get(10, TimeUnit.SECONDS);
				List<TopicName> topicNames = new ArrayList<>();
				for ( TopicListing t : listing) {
					String n = t.name();
					if (!internaltopics.contains(n)) {
						topicNames.add(TopicName.createViaEncoded(n));
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
			KafkaAPIdirect.addCustomProperties(consumerprops, getGlobalSettings().getKafkaConsumerProperties(), logger);
	
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
								if (record != null) {
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
			KafkaAPIdirect.addCustomProperties(consumerprops, getGlobalSettings().getKafkaConsumerProperties(), logger);

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
	public List<SchemaRegistryName> getSchemas() throws PipelineRuntimeException {
    	List<SchemaRegistryName> subjects = new ArrayList<>();
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
								SchemaRegistryName subject = SchemaRegistryName.createViaEncoded(
										schemadef.getSubject().substring(0, schemadef.getSubject().lastIndexOf('-')));
								if (schemadef.isDeleted()) {
									subjects.remove(subject);
								} else if (!subjects.contains(subject)) {
									if (!internalschemas.contains(subject.getEncodedName())) {
										subjects.add(subject);
									}
								}
							}
						}
						lastreadoffset = record.offset();
					}
				} while (lastreadoffset < lastoffset);
		    	return subjects;
			} catch (IOException e) {
				throw new PipelineRuntimeException("Reading the schema from the server failed", e, null);
			}
		} else {
			try {
				Response entityresponse = callRestfulservice(getSubjectsPath());
				if (entityresponse != null) {
					List<String> entityout = entityresponse.readEntity(new GenericType<List<String>>() { });
					if (entityout != null) {
						for (String s : entityout) {
							if (s.endsWith("-key")) {
								String subjectname = s.substring(0, s.lastIndexOf('-'));
								if (!internalschemas.contains(subjectname)) {
									SchemaRegistryName subject = SchemaRegistryName.createViaEncoded(subjectname);
									subjects.add(subject);
								}
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
    	ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(getProducerMetadataTopicName().getEncodedName(), 0, key, null);
    	producer.send(record);
	}

	@Override
	public void removeConsumerMetadata(String consumername) throws IOException {
    	GenericRecord keyrecord = new Record(consumermetadataschema.getKeySchema());
    	keyrecord.put(AVRO_FIELD_CONSUMERNAME, consumername);
		byte[] key = AvroSerializer.serialize(consumermetadataschema.getDetails().getKeySchemaID(), keyrecord);
    	ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(getConsumerMetadataTopicName().getEncodedName(), 0, key, null);
    	
    	producer.send(record);
	}
	
	@Override
	public void removeServiceMetadata(String servicename) throws IOException {
    	GenericRecord keyrecord = new Record(servicemetadataschema.getKeySchema());
    	keyrecord.put(AVRO_FIELD_SERVICENAME, servicename);
		byte[] key = AvroSerializer.serialize(servicemetadataschema.getDetails().getKeySchemaID(), keyrecord);
    	ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(getServiceMetadataTopicName().getEncodedName(), 0, key, null);
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
	    	
	    	ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(getProducerMetadataTopicName().getEncodedName(), 0, key, value);
	    	
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
	    	
	    	ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(getConsumerMetadataTopicName().getEncodedName(), 0, key, value);
	    	
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
	    	
	    	ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(getServiceMetadataTopicName().getEncodedName(), 0, key, value);
	    	
	    	this.producer.send(record);

    	}
	}


    @Override
	public ProducerMetadataEntity getProducerMetadata() throws PipelineRuntimeException {
    	long metadatamaxage = System.currentTimeMillis() - METADATAAGE;
		List<TopicPayload> records = getLastRecords(getProducerMetadataTopicName(), 10000); // list is sorted descending
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
		List<TopicPayload> records = getLastRecords(getConsumerMetadataTopicName(), 10000); // list is sorted descending
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
		List<TopicPayload> records = getLastRecords(getServiceMetadataTopicName(), 10000); // list is sorted descending
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
		KafkaAPIdirect.addCustomProperties(adminprops, getGlobalSettings().getKafkaAdminProperties(), logger);
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

	@Override
	public Map<String, LoadInfo> getLoadInfo(String producername, int instanceno) throws PipelineRuntimeException {
		Map<Integer, Map<String, LoadInfo>> info = getLoadInfo(producername);
		if (info != null) {
			return info.get(instanceno);
		} else {
			return null;
		}
	}
	
	@Override
	public Map<Integer, Map<String, LoadInfo>> getLoadInfo(String producername) throws PipelineRuntimeException {
		/*
		 * data contains all records, oldest is first
		 * Aggregates all recent transactions into the most recent state
		 */
		long lastreadtime = loadinfocache.getLastReadTime();
		/*
		 * The getAllRecordsSince() methods returns all records, oldest is first
		 */
		List<TopicPayload> data = getAllRecordsSince(getTransactionsTopicName(), lastreadtime, Integer.MAX_VALUE, null);
		for (TopicPayload d : data) {
			loadinfocache.setLastReadTime(d.getTimestamp());
			JexlRecord record = d.getValueRecord();
			String currentproducername = (String) record.get(PipelineAbstract.AVRO_FIELD_PRODUCERNAME);
			Integer currentinstanceno = (Integer) record.get(PipelineAbstract.AVRO_FIELD_PRODUCER_INSTANCE_NO);
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
				loadinfocache.put(currentproducername, currentinstanceno, schemaname, i);
			} else {
				loadinfocache.remove(currentproducername, currentinstanceno, schemaname);
			}
		}
		return loadinfocache.get(producername);
	}
	
	@Override
	public void resetInitialLoad(String producername, String schemaname, int producerinstance) throws IOException {
		Future<RecordMetadata> f = sendLoadStatus(producername, schemaname, producerinstance, null, false, "0", producer, null);
		try {
			f.get();
		} catch (InterruptedException | ExecutionException e) {
			throw new PipelineRuntimeException("Cannot send the reset-initial-load message to the transactionlog", e, null);
		}
	}
	
	@Override
	public void rewindDeltaLoad(String producername, int producerinstance, String transactionid) throws IOException {
		Future<RecordMetadata> f = sendLoadStatus(producername, PipelineAbstract.ALL_SCHEMAS, producerinstance, 0L, true, transactionid, producer, null);
		try {
			f.get();
		} catch (InterruptedException | ExecutionException e) {
			throw new PipelineRuntimeException("Cannot send the reset-initial-load message to the transactionlog", e, null);
		}
	}


	/**
	 * Adds a status row to the transaction log topic for a provided producer to make sure it is sent as last.
	 * Contains all end-offsets of all topic partitions.
	 * 
	 * @param producername responsible for the transaction 
	 * @param schemaname the transaction is created for - needed for initial loads
	 * @param producerinstance creating the transaction
	 * @param rowcount column
	 * @param finished successfully
	 * @param transactionid to use
	 * @param producerchannel is the Kafka producer the message should be sent with
	 * @param offsets information about the highest offsets used per topic and partition
	 * @return send future
	 * @throws IOException in case of error
	 */
	Future<RecordMetadata> sendLoadStatus(String producername, String schemaname, int producerinstance, 
			Long rowcount, boolean finished, String transactionid,
			KafkaProducer<byte[], byte[]> producerchannel, Map<String, Map<Integer, Long>> offsets) throws IOException {
		JexlRecord keyrecord = new JexlRecord(producertransactionschema.getKeySchema());
		keyrecord.set(PipelineAbstract.AVRO_FIELD_PRODUCERNAME, producername);
		keyrecord.set(PipelineAbstract.AVRO_FIELD_PRODUCER_INSTANCE_NO, producerinstance);
		keyrecord.set(KafkaAPIdirect.AVRO_FIELD_SCHEMANAME, schemaname);
		JexlRecord valuerecord = new JexlRecord(producertransactionschema.getValueSchema());
		valuerecord.set(PipelineAbstract.AVRO_FIELD_PRODUCERNAME, producername);
		valuerecord.set(PipelineAbstract.AVRO_FIELD_PRODUCER_INSTANCE_NO, producerinstance);
		valuerecord.set(KafkaAPIdirect.AVRO_FIELD_SCHEMANAME, schemaname);
		valuerecord.set(KafkaAPIdirect.AVRO_FIELD_SOURCE_TRANSACTION_IDENTIFIER, transactionid);
		valuerecord.set(KafkaAPIdirect.AVRO_FIELD_LASTCHANGED, System.currentTimeMillis());
		valuerecord.set(KafkaAPIdirect.AVRO_FIELD_ROW_COUNT, rowcount);
		valuerecord.set(KafkaAPIdirect.AVRO_FIELD_WAS_SUCCESSFUL, finished);
		
		if (offsets != null) {
			List<JexlRecord> topicoffsets = new ArrayList<>();
			Schema topicoffsetschema = IOUtils.getBaseSchema(producertransactionschema.getValueSchema().getField(AVRO_FIELD_TOPICOFFSETTABLE).schema()).getElementType();
			Schema partitionoffsetschema = IOUtils.getBaseSchema(topicoffsetschema.getField(AVRO_FIELD_OFFSETTABLE).schema()).getElementType();
			for (String topic : offsets.keySet()) {
				Map<Integer, Long> partitionoffset = offsets.get(topic);
				JexlRecord topicrecord = new JexlRecord(topicoffsetschema);
				topicrecord.put(AVRO_FIELD_TOPICNAME, topic);
				List<JexlRecord> partitionoffsets = new ArrayList<>();
				for (Integer partition : partitionoffset.keySet()) {
					JexlRecord partitionrecord = new JexlRecord(partitionoffsetschema);
					partitionrecord.put(AVRO_FIELD_PARTITON, partition);
					partitionrecord.put(AVRO_FIELD_PARTITON_OFFSET, partitionoffset.get(partition));
					partitionoffsets.add(partitionrecord);
				}
				topicrecord.put(AVRO_FIELD_OFFSETTABLE, partitionoffsets);
				topicoffsets.add(topicrecord);
			}
			valuerecord.put(AVRO_FIELD_TOPICOFFSETTABLE, topicoffsets);
		}
		
		byte[] key = AvroSerializer.serialize(producertransactionschema.getDetails().getKeySchemaID(), keyrecord);
		byte[] value = AvroSerializer.serialize(producertransactionschema.getDetails().getValueSchemaID(), valuerecord);
		ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(
				getTransactionsTopicName().getEncodedName(), null, key, value);
		return producerchannel.send(record);
	}
	
	@Override
	public void rewindConsumer(ConsumerProperties props, long epoch) throws IOException {
		ListConsumerGroupOffsetsResult offsets = admin.listConsumerGroupOffsets(props.getName());
		if (offsets != null) {
			try {
				Map<TopicPartition, OffsetAndMetadata> partitions = offsets.partitionsToOffsetAndMetadata().get(20, TimeUnit.SECONDS);
				if (partitions != null) {
					Map<TopicPartition, OffsetSpec> requestedoffsets = new HashMap<>();
					for (TopicPartition partition : partitions.keySet()) {
						requestedoffsets.put(partition, OffsetSpec.forTimestamp(epoch));
					}
					ListOffsetsResult result = admin.listOffsets(requestedoffsets);
					if (result != null) {
						Map<TopicPartition, ListOffsetsResultInfo> offsetlist = result.all().get();
						Map<TopicPartition, OffsetAndMetadata> l = new HashMap<>();
						for (TopicPartition partition : offsetlist.keySet()) {
							ListOffsetsResultInfo info = offsetlist.get(partition);
							l.put(partition, new OffsetAndMetadata(info.offset()));
						}
						admin.alterConsumerGroupOffsets(props.getName(), l );
					}
				}
			} catch (TimeoutException | InterruptedException | ExecutionException e) {
				throw new PipelineCallerException(
						"Failed to rewind the Consumer",
						e,
						"Requires all consumers of that name to be shutdown, even if running on a different server",
						props.getName());
			}
		}
	}

	@Override
	public void setGlobalSettings(GlobalSettings settings) {
		super.setGlobalSettings(settings);
	}
}
