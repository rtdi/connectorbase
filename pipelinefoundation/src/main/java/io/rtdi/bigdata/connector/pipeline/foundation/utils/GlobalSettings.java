package io.rtdi.bigdata.connector.pipeline.foundation.utils;

import java.util.Map.Entry;
import java.util.Properties;

import io.rtdi.bigdata.connector.pipeline.foundation.SchemaRegistryName;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;

public class GlobalSettings {
	private String ui5url = "https://openui5.hana.ondemand.com/resources/sap-ui-core.js";
	private String companyname = null;
	private String pipelineapi;
	private String connectorhelpurl;
	private String subjectspath;
	private TopicName schemaregistrytopicname;
	private TopicName transactionstopicname;
	private TopicName producermetadatatopicname;
	private TopicName consumermetadatatopicname;
	private TopicName servicemetadatatopicname;
	private SchemaRegistryName transactionsschemaname;
	private SchemaRegistryName producermetadataschemaname;
	private SchemaRegistryName consumermetadataschemaname;
	private SchemaRegistryName servicemetadataschemaname;
	private Properties kafkaconnectionproperties = new Properties();
	private Properties kafkaproducerproperties = new Properties();
	private Properties kafkaconsumerproperties = new Properties();
	private Properties kafkaadminproperties = new Properties();
	private Properties kafkastreamsproperties = new Properties();
	
	public static final String KAFKANAMESPACE_CONNECTION = "kafka.customproperties.connection";
	public static final String KAFKANAMESPACE_PRODUCER = "kafka.customproperties.producer";
	public static final String KAFKANAMESPACE_CONSUMER = "kafka.customproperties.consumer";
	public static final String KAFKANAMESPACE_ADMIN = "kafka.customproperties.admin";
	public static final String KAFKANAMESPACE_STREAMS = "kafka.customproperties.streams";
	
	public GlobalSettings() {
	}

	public GlobalSettings(Properties props) {
		if (props != null) {
			ui5url = props.getProperty("ui5url");
			companyname = props.getProperty("companyname");
			pipelineapi = props.getProperty("api");
			connectorhelpurl = props.getProperty("connectorhelpurl");
			schemaregistrytopicname = getTopicName(props, "topic.schemaregistry");
			transactionstopicname = getTopicName(props, "topic.transactions");
			producermetadatatopicname = getTopicName(props, "topic.producermetadata");
			consumermetadatatopicname = getTopicName(props, "topic.consumermetadata");
			servicemetadatatopicname = getTopicName(props, "topic.servicemetadata");
			transactionsschemaname = getSchemaName(props, "schema.transactions");
			producermetadataschemaname = getSchemaName(props, "schema.producermetadata");
			consumermetadataschemaname = getSchemaName(props, "schema.consumermetadata");
			servicemetadataschemaname = getSchemaName(props, "schema.servicemetadata");
			subjectspath = props.getProperty("schemaregistry.subjectspath");
			for ( Entry<Object, Object> e : props.entrySet()) {
				String key = e.getKey().toString();
				String value = e.getValue().toString();
				if (key.startsWith(KAFKANAMESPACE_CONNECTION)) {
					addCustomProperty(kafkaconnectionproperties, KAFKANAMESPACE_CONNECTION, key, value);
				} else if (key.startsWith(KAFKANAMESPACE_PRODUCER)) {
					addCustomProperty(kafkaproducerproperties, KAFKANAMESPACE_PRODUCER, key, value);
				} else if (key.startsWith(KAFKANAMESPACE_CONSUMER)) {
					addCustomProperty(kafkaconsumerproperties, KAFKANAMESPACE_CONSUMER, key, value);
				} else if (key.startsWith(KAFKANAMESPACE_ADMIN)) {
					addCustomProperty(kafkaadminproperties, KAFKANAMESPACE_ADMIN, key, value);
				} else if (key.startsWith(KAFKANAMESPACE_STREAMS)) {
					addCustomProperty(kafkastreamsproperties, KAFKANAMESPACE_STREAMS, key, value);
				}
			}
		}
	}
	
	private void addCustomProperty(Properties customprops, String namespace, String key, String value) {
		String reducedkey = key.substring(namespace.length());
		customprops.put(reducedkey, value);
	}

	private static TopicName getTopicName(Properties props, String name) {
		String s = props.getProperty(name);
		if (s != null) {
			return TopicName.create(s);
		} else {
			return null;
		}
	}

	private static SchemaRegistryName getSchemaName(Properties props, String name) {
		String s = props.getProperty(name);
		if (s != null) {
			return SchemaRegistryName.create(s);
		} else {
			return null;
		}
	}

	public String getUi5url() {
		return ui5url;
	}

	public void setUi5url(String ui5url) {
		this.ui5url = ui5url;
	}

	public String getCompanyName() {
		return companyname;
	}

	public void setCompanyName(String companyname) {
		this.companyname = companyname;
	}

	public String getPipelineAPI() {
		return pipelineapi;
	}

	public void setPipelineAPI(String classname) {
		this.pipelineapi = classname;
	}

	public String getConnectorHelpURL() {
		return connectorhelpurl;
	}

	public TopicName getSchemaRegistryTopicName() {
		return schemaregistrytopicname;
	}

	public TopicName getTransactionsTopicName() {
		return transactionstopicname;
	}

	public TopicName getProducerMetadataTopicName() {
		return producermetadatatopicname;
	}

	public TopicName getConsumerMetadataTopicName() {
		return consumermetadatatopicname;
	}

	public TopicName getServiceMetadataTopicName() {
		return servicemetadatatopicname;
	}

	public SchemaRegistryName getTransactionsSchemaName() {
		return transactionsschemaname;
	}

	public SchemaRegistryName getProducerMetadataSchemaName() {
		return producermetadataschemaname;
	}

	public SchemaRegistryName getConsumerMetadataSchemaName() {
		return consumermetadataschemaname;
	}

	public SchemaRegistryName getServiceMetadataSchemaName() {
		return servicemetadataschemaname;
	}

	public String getSubjectPath() {
		return subjectspath;
	}

	public Properties getKafkaConnectionProperties() {
		return kafkaconnectionproperties;
	}

	public Properties getKafkaProducerProperties() {
		return kafkaproducerproperties;
	}

	public Properties getKafkaConsumerProperties() {
		return kafkaconsumerproperties;
	}

	public Properties getKafkaAdminProperties() {
		return kafkaadminproperties;
	}

	public Properties getKafkaStreamsProperties() {
		return kafkastreamsproperties;
	}

}