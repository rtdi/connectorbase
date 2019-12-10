# Avro data types

Avro supports primitive datatypes like a String and based on those higher level data types can be built, the logical datatype, like a varchar(n). 
Some logical data types are provided by Avro (decimal, time, date) and others are needed for the connectors to provide extra metadata.

Hence as a result all handling with datatypes need to check the type, a primitive, an Avro provided logical data type or a connector logical type. 
To simplify that, this package contains classes for all three categories hiding the differences.

Note: Avro logical types need to be registered in Avro prior to use. see LogicalDataTypesRegistry.registerAll().   