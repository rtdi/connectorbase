package io.rtdi.bigdata.connectors.pipeline.kafkadirect;

import java.io.File;
import java.util.List;

import io.rtdi.bigdata.connector.pipeline.foundation.entity.KeyValue;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.PipelineConnectionProperties;


/**
 * In order to connect to Kafka directly this KafkaConnectionProperties object does provide 
 * <ul><li>Kafka Bootstrap Server list</li>
 * <li>optionally Kafka's Schema Registry URL</li></ul>
 * 
 * Note: In case the Kafka Connection is used by the KafkaAPIdirectAllTenants the TenantID can be NULL.
 *
 */
public class KafkaConnectionProperties extends PipelineConnectionProperties {
	private static final String KAFKABOOTSTRAPSERVERS = "kafka.bootstrapservers";
	private static final String KAFKASCHEMAREGISTRYURL = "kafka.schemaregistry.url";
	// private static final String SECURITYPROTOCOL = "kafka.security.protocol";
	private static final String APIKEY = "kafka.api.key";
	private static final String APISECRET = "kafka.api.secret";
	private static final String KAFKASCHEMAREGISTRYKEY = "kafka.schemaregistry.security.key";
	private static final String KAFKASCHEMAREGISTRYSECRET = "kafka.schemaregistry.security.secret";
	public static final String APINAME = "Kafka";
	public static final String KAFKASASLMECHANISM = "kafka.sasl.mechanism";
	public static final String KAFKASASLMECHANISM_PLAIN = "PLAIN";
	public static final String KAFKASASLMECHANISM_SCRAM = "SCRAM";
	public static List<KeyValue> options = List.of(new KeyValue(KAFKASASLMECHANISM_PLAIN, KAFKASASLMECHANISM_PLAIN), new KeyValue(KAFKASASLMECHANISM_SCRAM, KAFKASASLMECHANISM_SCRAM));

	
	/**
	 * Create a new KafkaConnectionProperties object with the list of available properties. 
	 */
	public KafkaConnectionProperties() {
		super(APINAME);
		properties.addStringProperty(KAFKABOOTSTRAPSERVERS, "Bootstrap Server list", "A comma separated list of kafka servers", null, null, false);
		properties.addStringProperty(APIKEY, "Optional cluster key", "The SASL security key", null, null, true);
		properties.addPasswordProperty(APISECRET, "Optional cluster secret string", "The SASL secret matching the key", null, null, true);
		properties.addStringSelectorProperty(KAFKASASLMECHANISM, "SASL mechanism", "If SASL key is provided this method defines the security protocol used", null, KAFKASASLMECHANISM_PLAIN, true, options);
		properties.addStringProperty(KAFKASCHEMAREGISTRYURL, "Optional schema registry url", "The url to the Kafka schema registry service or null", null, null, true);
		properties.addStringProperty(KAFKASCHEMAREGISTRYKEY, "Optional schema registry key", "The key to login to the schema registry", null, null, true);
		properties.addPasswordProperty(KAFKASCHEMAREGISTRYSECRET, "Optional schema registry secret string", "The matching secret to login to the schema registry", null, null, true);
	}

	public KafkaConnectionProperties(File connectordir) throws PropertiesException {
		this();
		this.read(connectordir);
	}

	/**
	 * Create a new KafkaConnectionProperties object with the list of available properties and set their values.
	 * 
	 * @param bootstrapservers list of Kafka servers
	 * @param schemaregistry url or null
	 * @throws PropertiesException if any of the properties are wrong
	 */
	public KafkaConnectionProperties(String bootstrapservers, String schemaregistry) throws PropertiesException {
		this();
		properties.setProperty(KAFKABOOTSTRAPSERVERS, bootstrapservers);
		properties.setProperty(KAFKASCHEMAREGISTRYURL, schemaregistry);
	}

	public void setKafkaSecurity(String key, String secret) throws PropertiesException {
		properties.setProperty(APIKEY, key);
		properties.setProperty(APISECRET, secret);
	}

	public void setSchemaRegistrySecurity(String key, String secret) throws PropertiesException {
		properties.setProperty(KAFKASCHEMAREGISTRYKEY, key);
		properties.setProperty(KAFKASCHEMAREGISTRYSECRET, secret);
	}

	/**
	 * @return The Kafka Bootstrap Server list
	 */
	public String getKafkaBootstrapServers() {
		return properties.getStringPropertyValue(KAFKABOOTSTRAPSERVERS);
	}
	
	public String getKafkaSchemaRegistry() {
		return properties.getStringPropertyValue(KAFKASCHEMAREGISTRYURL);
	}
	
	public String getKafkaSASLMechanism() {
		return properties.getStringPropertyValue(KAFKASASLMECHANISM);
	}

	public String getKafkaAPIKey() {
		return properties.getStringPropertyValue(APIKEY);
	}
	public String getKafkaAPISecret() {
		return properties.getPasswordPropertyValue(APISECRET);
	}
	public String getKafkaSchemaRegistryKey() {
		return properties.getStringPropertyValue(KAFKASCHEMAREGISTRYKEY);
	}
	public String getKafkaSchemaRegistrySecret() {
		return properties.getPasswordPropertyValue(KAFKASCHEMAREGISTRYSECRET);
	}

}
