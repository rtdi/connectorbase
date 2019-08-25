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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.jackson.JacksonFeature;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.rtdi.bigdata.connector.pipeline.foundation.AvroDeserialize;
import io.rtdi.bigdata.connector.pipeline.foundation.AvroSerializer;
import io.rtdi.bigdata.connector.pipeline.foundation.IOUtils;
import io.rtdi.bigdata.connector.pipeline.foundation.PipelineServerAbstract;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaName;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicPayload;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicUtil;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ConsumerMetadataEntity;
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
import io.rtdi.bigdata.connector.properties.ConsumerProperties;
import io.rtdi.bigdata.connector.properties.ProducerProperties;
import io.rtdi.bigdata.connectors.pipeline.kafkadirect.schemaentity.Converter;
import io.rtdi.bigdata.connectors.pipeline.kafkadirect.schemaentity.SchemaIdResponse;
import io.rtdi.bigdata.connectors.pipeline.kafkadirect.schemaentity.SchemaKey;
import io.rtdi.bigdata.connectors.pipeline.kafkadirect.schemaentity.SchemaRegistryKey;
import io.rtdi.bigdata.connectors.pipeline.kafkadirect.schemaentity.SchemaValue;

public class KafkaServer extends PipelineServerAbstract<KafkaConnectionProperties, TopicHandler, ProducerSessionKafkaDirect, ConsumerSessionKafkaDirect> {
	private static final String SERVICE_METADATA = "ServiceMetadata";
	private static final String CONSUMER_METADATA = "ConsumerMetadata";
	private static final String PRODUCER_METADATA = "ProducerMetadata";
	private static final String SCHEMA_TOPIC_NAME = "_schemas";
	public static final String AVRO_FIELD_SOURCE_TRANSACTION_IDENTIFIER = "SourceTransactionIdentifier";
	public static final String AVRO_FIELD_PRODUCERNAME = "ProducerName";
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
	private static final long METADATAAGE = 6*3600*1000;
		
	protected String bootstrapserver;
	private KafkaProducer<byte[], byte[]> producer;
	private AdminClient admin;
	private WebTarget target;

	
	private Map<String, TopicName> tenantproducermetadataname = new HashMap<>();
	private Map<String, TopicName> tenantconsumermetadataname = new HashMap<>();
	private Map<String, TopicName> tenantservicemetadataname = new HashMap<>();
	
	private Cache<Integer, Schema> schemaidcache = Caffeine.newBuilder().expireAfterAccess(Duration.ofMinutes(30)).maximumSize(1000).build();
	private Cache<SchemaName, SchemaHandler> schemacache = Caffeine.newBuilder().expireAfterAccess(Duration.ofMinutes(31)).maximumSize(1000).build();
	

    private Map<String, Object> consumerprops = new HashMap<>(); // These are used for admin tasks only, not to read data
    
	private SchemaHandler producermetadataschema;
	private SchemaHandler consumermetadataschema;
	private SchemaHandler servicemetadataschema;
	
	private static Schema producerkeyschema = SchemaBuilder.builder()
			.record("ProducerMetadataKey")
			.fields()
			.requiredString(AVRO_FIELD_PRODUCERNAME)
			.endRecord();
	private static Schema producervalueschema = SchemaBuilder.builder()
			.record("ProducerMetadataValue")
			.fields()
			.requiredString(AVRO_FIELD_PRODUCERNAME)
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
	
	private TopicPartition schemaregistrypartition = new TopicPartition(SCHEMA_TOPIC_NAME, 0);
	private Collection<TopicPartition> schemaregistrypartitions = Collections.singletonList(schemaregistrypartition);

	public KafkaServer(File rootdir) throws PropertiesException {
		this();
		setConnectionProperties(new KafkaConnectionProperties(rootdir));
	}
    
	public KafkaServer() {
		super();
		consumerprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		consumerprops.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10000);
		consumerprops.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
		
		consumerprops.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		consumerprops.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
	}
	
    public KafkaServer(KafkaConnectionProperties kafkaconnectionproperties) throws PropertiesException {
    	this();
    	setConnectionProperties(kafkaconnectionproperties);
    }
    
    @Override
	public void setConnectionProperties(KafkaConnectionProperties kafkaconnectionproperties) {
		super.setConnectionProperties(kafkaconnectionproperties);
    	bootstrapserver = kafkaconnectionproperties.getKafkaBootstrapServers();
		consumerprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
	}

	@Override
    public void open() throws PropertiesException {
    	if (producer == null) {
    		try {
		        Map<String, Object> producerprops = new HashMap<>();
				producerprops.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
				producerprops.put(ProducerConfig.ACKS_CONFIG, "all");
				producerprops.put(ProducerConfig.RETRIES_CONFIG, 0);
				producerprops.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
				producerprops.put(ProducerConfig.LINGER_MS_CONFIG, 1);
				producerprops.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
				producerprops.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
				producerprops.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
				
				Map<String, Object> adminprops = new HashMap<>();
				adminprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
		
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
	
					target = client.target(uri);
				} else {
					target = null;
				}

								
				SchemaName producermetadataschemaname = new SchemaName(null, PRODUCER_METADATA);
				producermetadataschema = getOrCreateSchema(producermetadataschemaname, null, producerkeyschema, producervalueschema);
				
				SchemaName consumermetadataschemaname = new SchemaName(null, CONSUMER_METADATA);
				consumermetadataschema = getOrCreateSchema(consumermetadataschemaname, null, consumerkeyschema, consumervalueschema);
				
				SchemaName servicemetadataschemaname = new SchemaName(null, SERVICE_METADATA);
				servicemetadataschema = getOrCreateSchema(servicemetadataschemaname, null, servicekeyschema, servicevalueschema);
				
				TopicName schemaregistrytopic = new TopicName(null, SCHEMA_TOPIC_NAME);
				createInternalTopic(schemaregistrytopic);
				
    		} catch (PipelineRuntimeException e) {
				close();
				throw e;
    		} catch (KafkaException e) {
    			// In case one of the three failed, close them. Else the threads remain open.
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
	
	protected void createMetadataTopics(String tenantid) throws PipelineRuntimeException {
		try {
			TopicName producermetadatatopicname = new TopicName(tenantid, PRODUCER_METADATA);
			createInternalTopic(producermetadatatopicname);
			TopicName consumermetadatatopicname = new TopicName(tenantid, CONSUMER_METADATA);
			createInternalTopic(consumermetadatatopicname);
			TopicName servicemetadatatopicname = new TopicName(tenantid, SERVICE_METADATA);
			createInternalTopic(servicemetadatatopicname);
			tenantproducermetadataname.put(tenantid, producermetadatatopicname);
			tenantconsumermetadataname.put(tenantid, consumermetadatatopicname);
			tenantservicemetadataname.put(tenantid, servicemetadatatopicname);
		} catch (PropertiesException e) {
			throw new PipelineRuntimeException("Creation of the Metadata topics failed", e, null);
		}
	}
	
	private TopicHandler createInternalTopic(TopicName topicname) throws PropertiesException {
		TopicHandler topichandler = getTopic(topicname);
		if (topichandler == null) {
			HashMap<String, String> props = new HashMap<String, String>();
			props.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
			return createTopic(topicname, 1, 1, props);
		} else {
			return topichandler;
		}
	}
	
      
    @Override
	public SchemaHandler getSchema(SchemaName kafkaschemaname) throws PropertiesException {
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
									if (schemadef.getSubject().equals(kafkaschemaname.toString() + "-key")) {
										keyschemadef = schemadef;
									} else if (schemadef.getSubject().equals(kafkaschemaname.toString() + "-value")) {
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
    			Response entityresponse = callRestfulservice("subjects/" + kafkaschemaname.getSchemaFQN() + "-key/versions/latest");
    			if (entityresponse != null) {
	    			SchemaValue keyschemadef = entityresponse.readEntity(SchemaValue.class);
					Schema keyschema = new Schema.Parser().parse(keyschemadef.getSchema());
					
	    			entityresponse = callRestfulservice("subjects/" + kafkaschemaname.getSchemaFQN() + "-value/versions/latest");
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

    @Override
    public SchemaHandler getOrCreateSchema(SchemaName schemaname, String description, Schema keyschema, Schema valueschema) throws PropertiesException {
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
		Future<RecordMetadata> future = getProducer().send(record);
		try {
			future.get(30, TimeUnit.SECONDS);
			schemaidcache.put(id, schema);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			throw new PipelineRuntimeException("Subject was not added successfully within 30 seconds", e, null, subjectname);
		}
   }
		
    @Override
	public TopicHandler createTopic(TopicName topic, int partitioncount, int replicationfactor, Map<String, String> props) throws PropertiesException {
		try {
			NewTopic t = new NewTopic(topic.toString(), partitioncount, (short) replicationfactor);
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
			throw new PipelineRuntimeException("Creation of the topic failed", e, null, topic.getTopicFQN());
		}
	}

	
    @Override
	public TopicHandler getTopic(TopicName kafkatopicname) throws PropertiesException {
		try {
			Collection<String> topicNames = Collections.singleton(kafkatopicname.getTopicFQN());
			DescribeTopicsResult result = admin.describeTopics(topicNames);
			Map<String, TopicDescription> v = result.all().get(10, TimeUnit.SECONDS);
			TopicDescription d = v.get(kafkatopicname.getTopicFQN());
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
			throw new PipelineTemporaryException("Topic cannot be read", e, kafkatopicname.getTopicFQN());
		} catch (ExecutionException e) {
			if (e.getCause() == null || e.getCause().getClass().equals(UnknownTopicOrPartitionException.class) == false) {
				throw new PipelineRuntimeException("Topic cannot be read", e, kafkatopicname.getTopicFQN());
			} else { // return null if the topic is not known yet
				return null;
			}
		}
	}

    @Override
	public List<String> getTopics(String tenantid) throws PipelineRuntimeException {
		if (tenantid == null) {
			throw new PipelineRuntimeException("TenantID cannot be null");
		} else {
			try {
				ListTopicsResult result = admin.listTopics();
				Collection<TopicListing> listing = result.listings().get(10, TimeUnit.SECONDS);
				List<String> topicNames = new ArrayList<>();
				String topicprefix = tenantid + "-";
				for ( TopicListing t : listing) {
					if (t.name().startsWith(topicprefix)) {
						String n = TopicUtil.extractTopicName(t.name());
						if (!n.equals(PRODUCER_METADATA) && !n.equals(CONSUMER_METADATA) && !n.equals(SERVICE_METADATA)) {
							topicNames.add(n);
						}
					}
				}
				Collections.sort(topicNames);
				return topicNames;
			} catch (InterruptedException | TimeoutException | PropertiesException e) {
				throw new PipelineTemporaryException("Reading the list of topics failed", e, tenantid);
			} catch (ExecutionException e) {
				throw new PipelineRuntimeException("Reading the list of topics failed", e, tenantid);
			}
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
	public List<TopicPayload> getLastRecords(TopicName kafkatopicname, long timestamp) throws PipelineRuntimeException {
		// TODO: Implementation missing
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
			consumerprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
			consumerprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
			consumerprops.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, count);
			consumerprops.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
	
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
				HashMap<Integer, Long> offsetstoreachtable = new HashMap<Integer, Long>();
				for (TopicPartition p : offsetmap.keySet()) {
					long offset = offsetmap.get(p);
					if (offset > 0) { // if the offset == 0 then there is no data. Skip reading that partition then
						offsetstoreachtable.put(p.partition(), offset-1); // The end offset is the offset of the next one to be produced, hence offset-1
						long from_offset = offset-(count/partitions.size());
						if (from_offset < 0) {
							from_offset = 0;
						}
						consumer.seek(p, from_offset);
					}
				}
				
				// An empty topic, that is one where no data is in any partition, can exit immediately with no data
				if (offsetstoreachtable.size() == 0) {
					return null;
				}
				
				long maxtimeout = System.currentTimeMillis() + 30000;
				
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
		
						int[] keyschemaid = new int[1];
						int[] valueschemaid = new int[1];
						GenericRecord keyrecord = null;
						GenericRecord valuerecord = null;
						try {
							keyrecord = AvroDeserialize.deserialize(record.key(), this, schemaidcache, keyschemaid);
						} catch (IOException e) {
							throw new PipelineRuntimeException("Cannot deserialize the Avro key record");
						}
						try {
							valuerecord = AvroDeserialize.deserialize(record.value(), this, schemaidcache, valueschemaid);
						} catch (IOException e) {
							throw new PipelineRuntimeException("Cannot deserialize the Avro value record");
						}
						TopicPayload data = new TopicPayload(kafkatopicname, record.offset(), record.partition(), record.timestamp(), keyrecord, valuerecord, keyschemaid[0], valueschemaid[0]);
						ret.add(0, data);
					}
				} while (offsetstoreachtable.size() != 0 && maxtimeout > System.currentTimeMillis());
				if (offsetstoreachtable.size() != 0) {
					throw new PipelineRuntimeException("Getting last records operation timed out for topic \"" + kafkatopicname.getTopicFQN() + "\"");
				} else {
					return ret;
				}
			} 
		}
	}
		
    @Override
	public List<String> getSchemas(String tenantid) throws PipelineRuntimeException {
    	List<String> subjects = new ArrayList<>();
    	String prefix = tenantid + "-";
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
							if (schemadef.getSubject().endsWith("-key") && schemadef.getSubject().startsWith(prefix)) {
								String subject = schemadef.getSubject().substring(0, schemadef.getSubject().lastIndexOf('-'));
								subject = TopicUtil.extractSchemaName(subject);
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
							if (s.endsWith("-key") && s.startsWith(prefix)) {
								String subject = s.substring(0, s.lastIndexOf('-'));
								subject = TopicUtil.extractSchemaName(subject);
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
    	
/*	private TopicHandler getorcreateNetworkTopic(TopicName topicnodes) throws ConnectorException {
		TopicHandler nodetopic = getTopicAllTenants(topicnodes);
		if (nodetopic == null) {
			Hashtable<String, String> props = new Hashtable<String, String>();
			props.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
			nodetopic = topicCreateAllTenants(topicnodes, 1, 1, props);
		}
		return nodetopic;
	}

	private SchemaHandler getorcreateNetworkNodeSchema(SchemaName schemanodes) throws ConnectorException {
		SchemaHandler nodeschema = getSchemaAllTenants(schemanodes);
		if (nodeschema == null) { 
			SchemaRecordBuilderKey keyschema = new SchemaRecordBuilderKey(TOPIC_INFRASTRUCTURE_NODES, "Nodes");
			keyschema.addColumn("nodeid", Type.STRING, "The node id", null, false);
			keyschema.addColumn("connectorname", Type.STRING, "connectorname", null, false);
			keyschema.build();
			
			SchemaRecordBuilderValue valueschema = new SchemaRecordBuilderValue(TOPIC_INFRASTRUCTURE_NODES, "Nodes");
			valueschema.addColumn("nodeid", Type.STRING, "The node id", null, false);
			valueschema.addColumn("label", Type.STRING, "node label", null, false);
			valueschema.addColumn("title", Type.STRING, "node title", null, false);
			valueschema.addColumn("color", Type.STRING, "node color", null, false);
			valueschema.addColumn("group", Type.STRING, "node groupname", null, true);
			valueschema.addColumn("connectorname", Type.STRING, "connectorname", null, true);
			valueschema.build();
			nodeschema = registerSchemaAllTenants(schemanodes, "Schema for the landscape nodes", keyschema.getSchema(), valueschema.getSchema());
		}
		return nodeschema;
	}

	private SchemaHandler getorcreateNetworkEdgeSchema(SchemaName schemanodes) throws ConnectorException {
		SchemaHandler edgeschema = getSchemaAllTenants(schemanodes);
		if (edgeschema == null) { 
			SchemaRecordBuilderKey keyschema = new SchemaRecordBuilderKey(TOPIC_INFRASTRUCTURE_EDGES, "Edges");
			keyschema.addColumn("from", Type.STRING, "from node id", null, false);
			keyschema.addColumn("to", Type.STRING, "to node id", null, false);
			keyschema.addColumn("connectorname", Type.STRING, "connectorname", null, true);
			keyschema.build();
			SchemaRecordBuilderValue valueschema = new SchemaRecordBuilderValue(TOPIC_INFRASTRUCTURE_EDGES, "Edges");
			valueschema.addColumn("from", Type.STRING, "from node id", null, false);
			valueschema.addColumn("to", Type.STRING, "to node id", null, false);
			valueschema.addColumn("connectorname", Type.STRING, "connectorname", null, true);
			valueschema.build();
			edgeschema = registerSchemaAllTenants(schemanodes, "Schema for all infrastructure edges", keyschema.getSchema(), valueschema.getSchema());
		}
		return edgeschema;
	}
	
	private void addNodeRecord(TopicHandler nodetopic, SchemaHandler nodeschema, String nodeid, String label, String title, String color, String groupname, String connectorname, long changetime) throws IOException, ConnectorException {
		Record keyrecord = new GenericData.Record(nodeschema.getKeySchema());
		keyrecord.put("nodeid", nodeid);
		keyrecord.put("connectorname", connectorname);
		Record valuerecord = new GenericData.Record(nodeschema.getValueSchema());
		valuerecord.put("nodeid", nodeid);
		valuerecord.put("label", label);
		valuerecord.put("title", title);
		valuerecord.put("color", color);
		valuerecord.put("connectorname", connectorname);
		valuerecord.put("group", groupname);
		valuerecord.put(IOUtils.SCHEMA_COLUMN_CHANGE_TIME, changetime);
		valuerecord.put(IOUtils.SCHEMA_COLUMN_CHANGE_TYPE, RowType.UPSERT.getIdentifer());
		byte[] key = AvroSerializer.serialize(SchemaMetadataDetails.getSchemaIDFromSchema(keyrecord.getSchema()), keyrecord);
		
		byte[] value = AvroSerializer.serialize(SchemaMetadataDetails.getSchemaIDFromSchema(valuerecord.getSchema()), valuerecord);
		ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(nodetopic.getTopicName().toString(), null, key, value);
		messagestatus.add(producer.send(record));
	}
	
	private void addEdgeRecord(TopicHandler edgetopic, SchemaHandler edgeschema, String from, String to, String connectorname, long changetime) throws IOException, ConnectorException {
		Record keyrecord = new GenericData.Record(edgeschema.getKeySchema());
		keyrecord.put("from", from);
		keyrecord.put("to", to);
		keyrecord.put("connectorname", connectorname);
		Record valuerecord = new GenericData.Record(edgeschema.getValueSchema());
		valuerecord.put("from", from);
		valuerecord.put("to", to);
		valuerecord.put("connectorname", connectorname);
		valuerecord.put(IOUtils.SCHEMA_COLUMN_CHANGE_TIME, changetime);
		valuerecord.put(IOUtils.SCHEMA_COLUMN_CHANGE_TYPE, RowType.UPSERT.getIdentifer());
		byte[] key = AvroSerializer.serialize(SchemaMetadataDetails.getSchemaIDFromSchema(keyrecord.getSchema()), keyrecord);
		
		byte[] value = AvroSerializer.serialize(SchemaMetadataDetails.getSchemaIDFromSchema(valuerecord.getSchema()), valuerecord);
		ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(edgetopic.getTopicName().toString(), null, key, value);
		messagestatus.add(producer.send(record));
	}
	
	private void networkcommit() throws ConnectorException {
		ProducerSessionKafkaDirect.checkMessageStatus(messagestatus);
	}

	public void updateInfrastructureAllTenants(LandscapeEntity childinfrastructure, String tenantid) throws ConnectorException {
		long changetime = System.currentTimeMillis();
		TopicName topicnodes = new TopicName(tenantid, TOPIC_INFRASTRUCTURE_NODES);
		TopicName topicedges = new TopicName(tenantid, TOPIC_INFRASTRUCTURE_EDGES);
		SchemaName schemanodes = new SchemaName(tenantid, TOPIC_INFRASTRUCTURE_NODES);
		SchemaName schemaedges = new SchemaName(tenantid, TOPIC_INFRASTRUCTURE_EDGES);
		TopicHandler nodetopic = getorcreateNetworkTopic(topicnodes);
		SchemaHandler nodeschema = getorcreateNetworkNodeSchema(schemanodes);
		
		TopicHandler edgetopic = getorcreateNetworkTopic(topicedges);
		SchemaHandler edgeschema = getorcreateNetworkEdgeSchema(schemaedges);
		
		try {
			deleteData(topicnodes, nodeschema, changetime, childinfrastructure.getconnectorname());
			deleteData(topicedges, edgeschema, changetime, childinfrastructure.getconnectorname());
			if (childinfrastructure.getNodes() != null) {
				for (NetworkNode node : childinfrastructure.getNodes()) {
					addNodeRecord(nodetopic, nodeschema, node.getId(), node.getLabel(), node.getTitle(), node.getColor(), node.getGroup(), childinfrastructure.getconnectorname(), changetime);
				}
			}
			if (childinfrastructure.getEdges() != null) {
				for (NetworkEdge edge : childinfrastructure.getEdges()) {
					addEdgeRecord(edgetopic, edgeschema, edge.getFrom(), edge.getTo(), childinfrastructure.getconnectorname(), changetime);
				}
				networkcommit();
			}
		} catch (IOException e) {
			throw new ConnectorException(e);
		}	
	}

	public void updateImpactLineageAllTenants(ImpactLineageEntity childimpactlineage, String tenantid) throws ConnectorException {
		long changetime = System.currentTimeMillis();
		TopicName topicnodes = new TopicName(tenantid, TOPIC_IMPACTLINEAGE_NODES);
		TopicName topicedges = new TopicName(tenantid, TOPIC_IMPACTLINEAGE_EDGES);
		SchemaName schemanodes = new SchemaName(tenantid, TOPIC_IMPACTLINEAGE_NODES);
		SchemaName schemaedges = new SchemaName(tenantid, TOPIC_IMPACTLINEAGE_EDGES);
		TopicHandler nodetopic = getorcreateNetworkTopic(topicnodes);
		SchemaHandler nodeschema = getorcreateNetworkNodeSchema(schemanodes);
		TopicHandler edgetopic = getorcreateNetworkTopic(topicedges);
		SchemaHandler edgeschema = getorcreateNetworkEdgeSchema(schemaedges);
		try {
			deleteData(topicnodes, nodeschema, changetime, childimpactlineage.getconnectorname());
			deleteData(topicedges, edgeschema, changetime, childimpactlineage.getconnectorname());
			if (childimpactlineage.getNodes() != null) {
				for (NetworkNode node : childimpactlineage.getNodes()) {
					addNodeRecord(nodetopic, nodeschema, node.getId(), node.getLabel(), node.getTitle(), node.getColor(), node.getGroup(), childimpactlineage.getconnectorname(), changetime);
				}
			}
			if (childimpactlineage.getEdges() != null) {
				for (NetworkEdge edge : childimpactlineage.getEdges()) {
					addEdgeRecord(edgetopic, edgeschema, edge.getFrom(), edge.getTo(), childimpactlineage.getconnectorname(), changetime);
				}
				networkcommit();
			}
		} catch (IOException e) {
			throw new ConnectorException(e);
		}	
	}
	
	private void deleteData(TopicName kafkatopicname, SchemaHandler schema, long changetime, String connectorname) throws ConnectorException, IOException {
		Map<String, Object> consumerprops = new HashMap<>();
		consumerprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
		consumerprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		consumerprops.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 30);
		consumerprops.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);

		consumerprops.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		consumerprops.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		ArrayList<TopicPartition> partitions = new ArrayList<TopicPartition>();
		try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(consumerprops);) {
			List<PartitionInfo> partitioninfos = consumer.partitionsFor(kafkatopicname.toString());
			for (PartitionInfo p : partitioninfos) {
				partitions.add(new TopicPartition(p.topic(), p.partition()));
			}
			consumer.assign(partitions);
			consumer.seekToBeginning(consumer.assignment());
			ConsumerRecords<byte[], byte[]> records = consumer.poll(500);
			while (records.count() != 0) {
				Iterator<ConsumerRecord<byte[], byte[]>> recordsiterator = records.iterator();
				while (recordsiterator.hasNext()) {
					ConsumerRecord<byte[], byte[]> record = recordsiterator.next();
					
					Record valuerecord = AvroDeserialize.deserialize(record.value(), this);
					if (valuerecord.get("connectorname") != null && valuerecord.get("connectorname").equals(connectorname)) {
						valuerecord.put(IOUtils.SCHEMA_COLUMN_CHANGE_TIME, changetime);
						valuerecord.put(IOUtils.SCHEMA_COLUMN_CHANGE_TYPE, RowType.DELETE.getIdentifer());
						byte[] value = AvroSerializer.serialize(SchemaMetadataDetails.getSchemaIDFromSchema(valuerecord.getSchema()), valuerecord);
						
						ProducerRecord<byte[], byte[]> producerrecord = new ProducerRecord<byte[], byte[]>(kafkatopicname.toString(), null, record.key(), value);
						messagestatus.add(producer.send(producerrecord));
					}
				}
				records = consumer.poll(500);
			}
			networkcommit();
		}
	}
	*/


	/*
	public LandscapeEntity getInfrastructureAllTenants(String tenantid) throws ConnectorException, IOException {
		TopicName topicnodes = new TopicName(tenantid, TOPIC_INFRASTRUCTURE_NODES);
		TopicName topicedges = new TopicName(tenantid, TOPIC_INFRASTRUCTURE_EDGES);
		LandscapeEntity impactlineage = new LandscapeEntity();
		readNetworkEntity(impactlineage, topicnodes, topicedges);
		return impactlineage;
	}

	public ImpactLineageEntity getImpactLineageAllTenants(String tenantid) throws IOException, ConnectorException {
		TopicName topicnodes = new TopicName(tenantid, TOPIC_IMPACTLINEAGE_NODES);
		TopicName topicedges = new TopicName(tenantid, TOPIC_IMPACTLINEAGE_EDGES);
		ImpactLineageEntity impactlineage = new ImpactLineageEntity();
		readNetworkEntity(impactlineage, topicnodes, topicedges);
		return impactlineage;
	}
	
	public void readNetworkEntity(NetworkEntity network, TopicName topicnodes, TopicName topicedges) throws ConnectorException, IOException {
		try {
	        Map<String, Object> consumerprops = new HashMap<>();
			consumerprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
			consumerprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			consumerprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
			consumerprops.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10000);
			consumerprops.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 30000);
			consumerprops.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 60000);
			
			consumerprops.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
			consumerprops.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
			try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(consumerprops);) {
				ArrayList<TopicPartition> topics = new ArrayList<TopicPartition>();
				topics.add(new TopicPartition(topicnodes.toString(), 0));
				topics.add(new TopicPartition(topicedges.toString(), 0));
				consumer.assign(topics);
				ConsumerRecords<byte[], byte[]> records = consumer.poll(5000); // take all records you can get in 5 seconds. Not an ideal implementation.
				if (records != null) {
					Iterator<ConsumerRecord<byte[], byte[]>> iter = records.iterator();
					while (iter.hasNext()) {
						ConsumerRecord<byte[], byte[]> rec = iter.next();
						Record key = AvroDeserialize.deserialize(rec.key(), this);
						Record value = AvroDeserialize.deserialize(rec.value(), this);
						if (value == null || value.get(IOUtils.SCHEMA_COLUMN_CHANGE_TYPE).toString().equals(RowType.DELETE.getIdentifer())) {
							network.removeNode(key.get("nodeid").toString());
						} else if (rec.topic().equals(topicnodes.toString())) {
							String label = null;
							String title = null;
							String color = null;
							String group = null;
							if (value.get("nodeid") != null) {
								if (value.get("label") != null) label = value.get("label").toString();
								if (value.get("title") != null) title = value.get("title").toString();
								if (value.get("color") != null) color = value.get("color").toString();
								if (value.get("group") != null) group = value.get("group").toString();
								network.addNode(value.get("nodeid").toString(), label, title, color, group);
							}
						} else if (rec.topic().equals(topicedges.toString())) {
							if (value.get("from") != null && value.get("to") != null) {
								network.addEdge(value.get("from").toString(), value.get("to").toString());
							}
						}
					}
				}
			}
		} catch (KafkaException e) {
			throw new ConnectorException(e);
		}
	}
	*/

    @Override
	public void removeProducerMetadata(String producername, String tenantid) throws IOException {
    	GenericRecord keyrecord = new Record(producermetadataschema.getKeySchema());
    	keyrecord.put(AVRO_FIELD_PRODUCERNAME, producername);
		byte[] key = AvroSerializer.serialize(producermetadataschema.getDetails().getKeySchemaID(), keyrecord);
    	ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(this.getTenantProducerMetadataName(tenantid).getTopicFQN(), 0, key, null);
    	getProducer().send(record);
	}

    public KafkaProducer<byte[], byte[]> getProducer() {
    	return producer;
    }

	@Override
	public void removeConsumerMetadata(String consumername, String tenantid) throws IOException {
    	GenericRecord keyrecord = new Record(consumermetadataschema.getKeySchema());
    	keyrecord.put(AVRO_FIELD_CONSUMERNAME, consumername);
		byte[] key = AvroSerializer.serialize(consumermetadataschema.getDetails().getKeySchemaID(), keyrecord);
    	ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(this.getTenantConsumerMetadataName(tenantid).getTopicFQN(), 0, key, null);
    	
    	producer.send(record);
	}
	
    @Override
	public void removeServiceMetadata(String servicename, String tenantid) throws IOException {
    	GenericRecord keyrecord = new Record(servicemetadataschema.getKeySchema());
    	keyrecord.put(AVRO_FIELD_SERVICENAME, servicename);
		byte[] key = AvroSerializer.serialize(servicemetadataschema.getDetails().getKeySchemaID(), keyrecord);
    	ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(this.getTenantServiceMetadataName(tenantid).getTopicFQN(), 0, key, null);
    	getProducer().send(record);
	}

	
    @Override
	public void addProducerMetadata(String tenantid, ProducerEntity producer) throws IOException {
    	if (producer != null) {
        	GenericRecord keyrecord = new Record(producermetadataschema.getKeySchema());
        	keyrecord.put(AVRO_FIELD_PRODUCERNAME, producer.getProducerName());
        	GenericRecord valuerecord = new Record(producermetadataschema.getValueSchema());
        	valuerecord.put(AVRO_FIELD_PRODUCERNAME, producer.getProducerName());
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
	    	
	    	ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(this.getTenantProducerMetadataName(tenantid).getTopicFQN(), 0, key, value);
	    	
	    	this.producer.send(record);

    	}
	}

	@Override
	public void addConsumerMetadata(String tenantid, ConsumerEntity consumer) throws IOException {
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
	    	
	    	ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(getTenantConsumerMetadataName(tenantid).getTopicFQN(), 0, key, value);
	    	
	    	producer.send(record);
		}
	}
	
    @Override
	public void addServiceMetadata(String tenantid, ServiceEntity service) throws IOException {
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
	    	
	    	ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(this.getTenantServiceMetadataName(tenantid).getTopicFQN(), 0, key, value);
	    	
	    	this.producer.send(record);

    	}
	}


    @Override
	public ProducerMetadataEntity getProducerMetadata(String tenantid) throws PipelineRuntimeException {
    	long metadatamaxage = System.currentTimeMillis() - METADATAAGE;
		List<TopicPayload> records = getLastRecords(getTenantProducerMetadataName(tenantid), 10000); // list is sorted descending
		if (records != null && records.size() > 0) {
			List<ProducerEntity> producerlist = new ArrayList<>();
			Set<String> uniqueproducers = new HashSet<>();
			for (TopicPayload producerdata : records) {
				Object o = producerdata.getValueRecord().get(AVRO_FIELD_PRODUCERNAME);
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
	public ConsumerMetadataEntity getConsumerMetadata(String tenantid) throws PipelineRuntimeException {
    	long metadatamaxage = System.currentTimeMillis() - METADATAAGE;
		List<TopicPayload> records = getLastRecords(getTenantConsumerMetadataName(tenantid), 10000); // list is sorted descending
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
	public ServiceMetadataEntity getServiceMetadata(String tenantid) throws PipelineRuntimeException {
    	long metadatamaxage = System.currentTimeMillis() - METADATAAGE;
		List<TopicPayload> records = getLastRecords(getTenantServiceMetadataName(tenantid), 10000); // list is sorted descending
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


	private TopicName getTenantConsumerMetadataName(String tenantid) throws PipelineRuntimeException {
		TopicName t = tenantconsumermetadataname.get(tenantid);
		if (t == null) {
			createMetadataTopics(tenantid);
			return tenantconsumermetadataname.get(tenantid);
		} else {
			return t;
		}
	}

	private TopicName getTenantProducerMetadataName(String tenantid) throws PipelineRuntimeException {
		TopicName t = tenantproducermetadataname.get(tenantid);
		if (t == null) {
			createMetadataTopics(tenantid);
			return tenantproducermetadataname.get(tenantid);
		} else {
			return t;
		}
	}

	private TopicName getTenantServiceMetadataName(String tenantid) throws PipelineRuntimeException {
		TopicName t = tenantservicemetadataname.get(tenantid);
		if (t == null) {
			createMetadataTopics(tenantid);
			return tenantservicemetadataname.get(tenantid);
		} else {
			return t;
		}
	}

	@Override
	public void loadConnectionProperties(File webinfdir) throws PropertiesException {
		setConnectionProperties(new KafkaConnectionProperties(webinfdir));
	}

	@Override
	public ProducerSessionKafkaDirect createNewProducerSession(String tenantid) throws PropertiesException {
		return new ProducerSessionKafkaDirect(new ProducerProperties("default"), tenantid, this);
	}

	@Override
	public ConsumerSessionKafkaDirect createNewConsumerSession(String consumername, String topicpattern, String tenantid) throws PropertiesException {
		ConsumerProperties props = new ConsumerProperties(consumername);
		props.setTopicPattern(topicpattern);
		return new ConsumerSessionKafkaDirect(props, tenantid, this);
	}

	@Override
	public void writeConnectionProperties(File webinfdir) throws PropertiesException {
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
	public String getConnectionLabel() {
		return getAPIProperties().getKafkaBootstrapServers();
	}

}
