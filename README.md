# Kafka RTDI Big Data Connectors

This project aims to building Real Time Data Integration Connectors (RTDI Connectors) enabling business users to load and consume business data.

Example: A user needs to load a local CSV file to a Kafka instance running in the cloud. To enable that, the user has to solve multiple issues starting with "How to configure the network" to "Who converts the file content?" and "How can the business user specify the file format definitions visually".

For an end user centric point of view, checkout the [product page](https://rtdi.io/software/big-data-connectors/) with its live demo systems.

## Relationship with Kafka Connect

The obvious first choice would have been to enhance Kafka Connect. But Kafka Connect aims at a different audience and use cases. It is about IT professionals loading data with maximum performance. In addition most Kafka Connect Connectors do not support basic functionalities users would expect. Example: How does a reliable delta work in the JDBC Kafka Connect?

Key differences

- Kafka Connect runs in a cluster, the RTDI Connectors are web applications, e.g. wrapped into Docker containers
- Kafka Connect is configured via property files, the RTDI-Connectors have UIs to configure and maintain them
- Kafka Connect transfers low level data like tables, the RTDI-Connectors transfer Business Objects like a Sales Order
- Kafka Connect does send data as is, the RTDI-Connectors add common metadata needed for the bigger picture
- Kafka Connect is usually limited to one-topic-one-schema, the RTDI-Connectors use the schema to identify the structure and the topic to handle the order. If a database is replicated and transactional order is important, all data needs to go through a single topic (partition) to preserve the order. It is the users choice to balance parallel processing against transactional order.

Aside from the differences, Kafka Connect and the RTDI-Connectors can work together, e.g. producing data with the RTDI-Connector and consuming with Kafka Connect, for example. The payload is an Avro schema in both cases, using the Kafka schema registry and are encoded the same way.

## The various modules

Note: All modules are available in **Maven Central**

Generally speaking there are two different module types, the Pipeline and the Connector. The Pipeline is used by the Connector to talk with the backend. The [pipelinehttp](https://github.com/rt-di/connectorbase/tree/master/pipelinehttp) module speaks to Kafka via https streams, using the [pipelinehttpserver](https://github.com/rt-di/connectorbase/tree/master/pipelinehttpserver) as bridge from https to the Kafka native protocol. This pipeline variant makes sense if Kafka is installed in the could and the source is on premise.

The [pipelinekafkatcp](https://github.com/rt-di/connectorbase/tree/master/pipelinekafkatcp) module is using the Kafka TCP protocol and is useful in case Kafka and the sources are in the same network.

Connectors themselves are built on top of the [webappfoundation](https://github.com/rt-di/connectorbase/tree/master/webappfoundation) module, which in turn is based on a lower level api, the [connectorfoundation](https://github.com/rt-di/connectorbase/tree/master/connectorfoundation). The webapp foundation provides mostly UI related code, the connectorfoundation is about reading and storing configuration files, starting stopping the various threads, error handling etc.

The common basis including all the shared objects is the [pipelinefoundation](https://github.com/rt-di/connectorbase/tree/master/pipelinefoundation).

The [pipelinetest](https://github.com/rt-di/connectorbase/tree/master/pipelinetest) is an internal module, used to simulate a pipeline. It adds records to an in-memory cache without any long term persistence. Useful for test automation only.

## Sample Connectors

[RTDIDemoConnector](https://github.com/rtdi/RTDIDemoConnector): Simulates an ERP producer creating Material, Customer, Employee and SalesOrder Business Objects

[RTDIFileConnector](https://github.com/rtdi/RTDIFileConnector): Allows reading flat files in realtime (as soon as they appear in a directory), convert them into a different structure and configure the file format settings interactively.