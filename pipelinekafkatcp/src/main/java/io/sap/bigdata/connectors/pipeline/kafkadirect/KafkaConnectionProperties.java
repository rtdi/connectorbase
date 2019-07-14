package io.sap.bigdata.connectors.pipeline.kafkadirect;

import java.io.File;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.PipelineConnectionProperties;


/**
 * In order to connect to Kafka directly this KafkaConnectionProperties object does provide 
 * <UL><LI>Kafka Bootstrap Server list</LI>
 * <LI>Kakfa's Schema Registry URL</LI></UL>
 * 
 * Note: In case the Kafka Connection is used by the KafkaAPIdirectAllTenants the TenantID can be NULL.
 *
 */
public class KafkaConnectionProperties extends PipelineConnectionProperties {
	private static final String KAFKABOOTSTRAPSERVERS = "kafka.bootstrapservers";
	private static final String KAFKASCHEMAREGISTRYURL = "kafka.schemaregistry.url";
	
	
	/**
	 * Create a new KafkaConnectionProperties object with the list of available properties. 
	 */
	public KafkaConnectionProperties() {
		super("Kafka");
		properties.addStringProperty(KAFKABOOTSTRAPSERVERS, "Bootstrap Server list", "A comma separated list of kafka servers", null, null, false);
		properties.addStringProperty(KAFKASCHEMAREGISTRYURL, "Optional schema registry url", "The url to the Kafka schema registry service or null", null, null, true);
	}

	public KafkaConnectionProperties(File connectordir) throws PropertiesException {
		this();
		this.read(connectordir);
	}

	/**
	 * Create a new KafkaConnectionProperties object with the list of available properties and set their values.
	 * 
	 * @param bootstrapservers
	 * @param schemaregistry url or null
	 * @throws PipelinePropertiesException 
	 */
	public KafkaConnectionProperties(String bootstrapservers, String schemaregistry) throws PropertiesException {
		this();
		properties.setProperty(KAFKABOOTSTRAPSERVERS, bootstrapservers);
		properties.setProperty(KAFKASCHEMAREGISTRYURL, schemaregistry);
	}

	/**
	 * @return The Kafka Bootstrap Server list
	 * @throws PipelinePropertiesException 
	 */
	public String getKafkaBootstrapServers() {
		return properties.getStringPropertyValue(KAFKABOOTSTRAPSERVERS);
	}
	
	public String getKafkaSchemaRegistry() {
		return properties.getStringPropertyValue(KAFKASCHEMAREGISTRYURL);
	}
	
}
