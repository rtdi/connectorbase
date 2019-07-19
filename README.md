# Kafka RTDI-Connector Base

This project aims to allow building Real Time Data Integration Connectors (RTDI Connectors) which enable business users loading and consuming business data.

Example: A user needs to load local CSV file content to a Kafka instance running in the cloud. This requires multiple problems to be handled, from "How does a local file end up in a remote Kafka instance?" to "How can the business user specify the file format definitions visually".

## Difference to Kafka Connect

The obvious first choice would have been to enhance Kafka Connect. But it aims at a totally different use case, the IT person loading technical data with maximum parallelism. In addition most Kafka Connect Connectors do not support even the most basic functionalities. They look and feel more like a Proof of Concept than professional grade products.

Key differences

- Kafka Connect runs in a cluster, here Connectors are Web Applications
- Kafka Connect is configured via property files, the RTDI-Connectors have UIs to configure and maintain them
- Kafka Connect transfers low level data like tables, the RTDI-Connectors transfer Business Objects like a Sales Order
- Kafka Connect does send data as is, the RTDI-Connectors add common metadata needed for the bigger picture
- Kafka Connect is usually limited to one-topic-one-schema, the RTDI-Connectors use the schema to identify the structure and the topic to handle the order. If a database is replicated and transactional order is important, all data needs to go through a single topic (partition) to preserve the order. It is the users choice to balance parallel processing against transactional order.

Aside from the differences, Kafka Connect and the RTDI-Connectors can work together. To produce data with the RTDI-Connector and consume with Kafka Connect for example. The payload is an Avro schema in both cases, using the schema registry and encoded the same way.

## The various modules

Generally speaking there are two different module types, the Pipeline and the Connector. The Pipeline is used by the Connector to talk with the backend and there are different types. The [pipelinehttp](https://github.com/rt-di/connectorbase/tree/master/pipelinehttp) module speaks to Kafka via https, using the [pipelinehttpserver](https://github.com/rt-di/connectorbase/tree/master/pipelinehttpserver) as bridge from https to the Kafka native protocol. The [pipelinekafkatcp](https://github.com/rt-di/connectorbase/tree/master/pipelinekafkatcp) module on the other hand is using the Kafka TCP protocol directly and hence requires direct access to Kafka.

Connectors are built on top of the [webappfoundation](https://github.com/rt-di/connectorbase/tree/master/webappfoundation) module, which in turn is based on a lower level api, the [connectorfoundation](https://github.com/rt-di/connectorbase/tree/master/connectorfoundation).

The common basis including all the shared objects is the [pipelinefoundation](https://github.com/rt-di/connectorbase/tree/master/pipelinefoundation).

The [pipelinetest](https://github.com/rt-di/connectorbase/tree/master/pipelinetest) is an internal module, used to simulate a pipeline. It adds records to an in-memory cache without any long term persistence.

