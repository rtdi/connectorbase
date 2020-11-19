package io.rtdi.bigdata.connector.pipeline.foundation.utils;

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
		}
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

}