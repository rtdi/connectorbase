# Pipeline API foundation

The Pipeline API is the basis to write concrete implementations. It abstracts various server implementations into a simplified API. Most important, even if Apache Kafka is the primary targeted server, two implementations are possible: A direct TCP connection to Kafka and one via https where a webserver is the intermediary to Kafka.

## What is a Pipeline?

A Pipeline is a system with Producers adding Apache Avro records and Consumer reading those. A Transaction Log stores all records for a certain amount of time, e.g. the Kafka default week.

This architecture fits the real world use cases very well. How many producers for a certain data element are there? One, maybe two. How many consumers for this data element? A lot. The raw data store, the data warehouse, connected downstream databases, various services like data validation or alerting system and many more. If there are few producers and many consumers, it would make sense to prepare the data in the producer as much as possible so the consumers do not have to do the task individually. Right? 

Hence the system consists of

* A Schema Registry for Avro, a place where the data model of the Pipeline objects is stored.
* Topics (=Queues) where data is persisted.
* ProducerSessions allow adding data.
* ConsumerSessions allow consuming persisted data.
* Impact/Lineage directory to see who produces and who consumes data
* Landscape information where each component is located

A Pipeline is not a Publish/Subscribe message queue!

The main advantage of this model is that the Consumer is in charge of deciding what data it needs and when. For example...
* A Data Warehouse Consumer would read the data every 24 hours because the Data Warehouse should return stable data for today's revenue.
* A simple data preview can show the current data as is.
* A Data Lake writer might batch-write the data every hour or whenever the amount of data exceeds a row limit.
* A database writer can add the data with a latency in milliseconds.

Because the Pipeline server is backed by an user defined data model, the connectors need to have the ability to perform structure transformations. The idea here is that for a certain business object, say the customer master record, there are multiple sources. The ERP system contains the data for all customers buying, the web shop all registered users even if they have not bought anything yet. The user defines a data model for customer master holding all needed information and both producers have a mapping from their internal structure to this Pipeline data model. It could be a 1:1 mapping because the data model is the structure of the ERP table. It could be a simple mapping to rename columns. 


## Rules

### Avro for key and value
While Kafka supports different payload formats from strings to binary, the payload format here is Avro for keys and values. Always. Further more, the key fields are a subset of the value fields.
This might sound like a constraint it simplifies things for the user. And most important it imposes no limitations as even unstructured data contains structured information. 

Want to load text files? A text file consists of the free form text but also author, create date, last modify date, text format (mime type), character set used. In Avro that would be a flat structure with a few fields, the free form text goes into a String column encoded in UTF-8. Otherwise the consumers would not know the metadata of the file and every single consumer would need to transform the text from the source encoding into a common character set.

Want to load image data? A jpeg file has lots of metadata, the entire EXIF structure. Geographic location, camera model, exposure time, focal length,... If the producer offers these fields as part of the Avro data, the consumers do not have to.

### One topic can hold multiple schemas
For Kafka itself that is no problem. Every Avro message is serialized into a byte stream and within the first few bytes the id of the schema used is stored. Thus the consumer knows this is a customer master record, this is a sales order record.

Unfortunately most connectivity solutions like Kafka's own Kafka-Connect solution does not support that. They assume one topic = one schema. Not for a technical reason but to make coding these a tad easier.

But for data integration in the Kafka style, this separation is required in almost all scenarios. In Kafka, data within a single Topic/partition retains its order, but two topic/partitions are processed parallel and therefore in different sequence. A database producer finds a new customer record and a sales order for this customer. If the producer puts the data into two topics, the consumers have no control which one comes first, the customer master record or the sales order. So with a small but possible likeliness the order for a customer which does not exist yet is loaded into the target. Or a fraud service finds a sales order that has no customer record yet.
If on the other hand the customer master record and the sales order go into the same topic/partition, then the order is retained and based on the schema the consumer knows that the first is a customer master record, the second a sales order.

### All records have extra metadata
Information like when was the record produced, what was the record's origin, a hint if the record should be inserted, deleted or updated,... all this is added by the producer.
In addition every record has metadata about its quality and the transformations it did undergo. A producer reading CSV files can therefore add information about the reading. This line was parsed but had just 9 fields instead of the the expected 10 --> Warning. One field had been empty. It is a nullable-field so technically okay but still good to know.

The real power of this metadata comes when data is processed through multiple services. A validation service checking if the values are in valid ranges, an enrichment service deriving the address components out of an address line free form text, a ML service qualifying the sales order as A/B/C segment. All these services add metadata to the record. This allows for nice operational reports about the quality of the data and how it got processed through the pipeline.

### Extension concepts
Thanks to the Avro schema evolution concept, the schema can grow over the time. A new source system provides data as well, hence the schema gets extended by a few new fields. The other producers are not impacted as they have no data for that field - it will have a default value.
There are cases however where the producer needs to store extra data but modifying the data model would be too much. 

Example: A source system is using the code 0 and 1, the data model says the value for indicating online sales is "Direct Sales" vs "Indirect Sales". To play it safe, the source system's original code should be stored somewhere in the message still. Will not be used but might help for debugging and auditing the record later. For these types of data an Extension array is available, which is essentially just a key value pair record. Hence the producer can add the one record with key "Sourcefield SALES_TYPE" and value "1" to store the original data. 