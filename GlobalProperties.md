# global.properties file

Within the root folder containing the settings, a global.properties file should be placed. It contains information needed or the option to change global values.

[SourceCode](https://github.com/rtdi/connectorbase/blob/master/pipelinefoundation/src/main/java/io/rtdi/bigdata/connector/pipeline/foundation/utils/GlobalSettings.java)

Example content for the RulesService in the docker image:

    ui5url=/openui5/resources/sap-ui-core.js
    api=KafkaAPIdirect
    connectorhelpurl=https://github.com/rtdi/RTDIRulesService
    
## Supported properties

None of the values actually need to be set. Their defaults if no global.properties file is found are fine and all will work.
For docker images above three values do make sense to avoid reading the OpenUI5 library over the Internet instead of the local version.
The connectorhelpurl is used in the Login and the Connector Home page. If no value is provided, the corresponding links are not rendered.

The main reason for the global properties is to provide users a first level of customizations. For example the connectorhelpurl can point to an Intranet page instead. In most cases the docker values are fine.


| Name       | Description |
| ---------- | ----------- |
| ui5url | A link to the UI5 library to use. When not specified the latest SAP OpenUI5 version is used. For the docker images the locally deployed OpenUI5 version is specified. |
| companyname | Free form text for the company name. Sent with the usage statistic information. |
| pipelineapi | The default logic is to pick the first found PipelineAPI. In the docker images only the KafkaAPIdirect is copied by default. But if multiple are used, here the name can be specified. |
| connectorhelpurl | The root page of the connector help |
| topic.schemaregistry | If no schema registry is used, the Pipeline is using the topic _schemas or the name specified here. In most case the connection will have a schema registry end point and thus this value is not used. |
| topic.transactions | For producers to know where to restart from, each source transaction is stored in the topic "_producertransactions" or the name specified here. |
| topic.producermetadata | Producers also store metadata for impact/lineage diagrams. The topic name is "ProducerMetadata" or the name specified here. |
| topic.consumermetadata | similar for Consumers with the default topic name "ConsumerMetadata". |
| topic.servicemetadata | similar for Services with the default topic name "ServiceMetadata". |
| schema.transactions | The schema name used to store transaction metadata in the topic.transactions. |
| schema.producermetadata | similar for producers |
| schema.consumermetadata | similar for consumers |
| schema.servicemetadata | similar for services |
| kafka.customproperties.connection.* | adds the * part to the Kafka properties |
| kafka.customproperties.producer.* | adds the * part to the Kafka properties |
| kafka.customproperties.consumer.* | adds the * part to the Kafka properties |
| kafka.customproperties.admin.* | adds the * part to the Kafka properties |
| kafka.customproperties.streams.* | adds the * part to the Kafka properties |


Example for kafka.customproperties.connection.*:
When using the Kafka library for either a producer, consumer or stream, certain connection specific properties might need to be set, e.g. security.protocol to some value not supported by the UI. In that case, by adding a property `kafka.customproperties.connection.security.protocol` to the global properties file, the custom property is added to the properties object used when connecting to Kafka (without the prefix of course).
For cases where a property makes sense only to producers or consumers, the other two prefixes exist.
If a property is specified that the UI wants to set as well, e.g. the bootstrap.servers, the global properties value takes priority always.
