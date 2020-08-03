# Schema definition

While Kafka itself does not care about the schema and the payload, in order to build a proper end-to-end solution the producer should add some additional information.

## __change_type

The producer knows how to interpret incoming data. One producer might get new sensor data, hence it can flag the record as INSERT and thus the consumer will know to add that record.
Another producer knows that in some cases the data might be sent twice, either by accident during error handling or for a good reason because the data changed. For example a producer for file data does process a file and a few minutes later the same file is uploaded again with corrected data. Therefore the producer would mark all records as UPSERT and the consumers then know that data might get corrected. It is still up to the various consumers what to actually do with the records.
A database consumer might execute insert or upsert statements, a Hadoop consumer appends the data into the Hadoop file system but add the change type information as payload.

The  _"__change_type"_  column should therefore exist at the root level of each schema and the possible values and their use cases are described [here](https://github.com/rtdi/connectorbase/blob/master/pipelinefoundation/src/main/java/io/rtdi/bigdata/connector/pipeline/foundation/enums/RowType.java).

## Primary key information

For operations other than Insert and Truncate to work, the consumer needs to know the primary key columns. Therefore fields at the root level can have the additional property  _"__primary_key_column"_  set to true.

Example: 

	{
	   "type":"record",
	   "name":"Employee",
	   "fields":[
	      {
	         "name":"EmployeeNumber",
	         "type":{
	            "type":"string",
	         },
	         "__primary_key_column":true
	      },
	      {
	         "name":"Firstname",
	         "type":[
	            "null",
	            {
	               "type":"string",
	            }
	         ]
	      },
	   ]
	}

## Names in Avro can be simple text only

To allowed characters in Avro are very limited, more limited than source system allow. In a simple example where data is moved from one database to another, the user expects the table and column names to be the same, which is not possible if the Avro names do not support these names. Example: "Column-1" is a perfectly fine database column name.

Note: Avro field names are limited to [A-Za-z0-9_].

To resolve that issue, a field can have the additional property _"__originalname"_ where the proper name is preserved.

An [encoder class](https://github.com/rtdi/connectorbase/blob/master/pipelinefoundation/src/main/java/io/rtdi/bigdata/connector/pipeline/foundation/utils/NameEncoder.java) helps converting arbitrary field names into Avro fieldnames.

Example:

	{
	   "type":"record",
	   "name":"Employee",
	   "fields":[
	      {
	         "name":"FirstName",
	         "type":[
	            "null",
	            {
	               "type":"string",
	            }
	         ],
	         "__originalname":"First-Name"
	      },
	   ]
	}


## More detailed data types

Avro itself supports the basic Java datatypes like String only. This makes it very hard for the consumers to pick the proper data type, e.g. when the consumer writes into a database. Does this string contain ASCII chars only or Unicode? What is a maximum possible length?

As result, copying a source database table into a target database via Kafka would make the target table look completely different. The only correct choice for the consumer to load any Java String value is the NCLOB datatype which has performance and handling issues in databases.

Another problem with Avro is identifying the correct datatype. The code would need to look into the schema and find if that is a Logical Type. If yes, methods from the logical type are used for e.g. datatype conversion. If no, the base data types are used. If the logical type is a custom built type, the code path is yet another one.

The last problem with Avro is the data type conversion flexibility. Avro will simply fail when it expects a String but finds an Integer value although the conversion is obvious.

Example: The Avro decimal data type is a Avro Logical Type based on a byte array. A consumer would see the bytes [ 0x3a, 0x7d ] and must write that into the database as a decimal number.

To solve all of these problems custom logical types have been created for all typical Avro and JDBC data types. 

see [here](https://github.com/rtdi/connectorbase/tree/master/pipelinefoundation/src/main/java/io/rtdi/bigdata/connector/pipeline/foundation/avrodatatypes)





# Full example

	{
	   "type":"record",
	   "name":"Employee",
	   "fields":[
	      {
	         "name":"__change_type",
	         "type":{
	            "type":"string",
	            "logicalType":"VARCHAR",
	            "length":1
	         },
	         "__originalname":"__change_type",
	         "__internal":true,
	         "__technical":true
	      },
	      {
	         "name":"__change_time",
	         "type":{
	            "type":"long",
	            "logicalType":"timestamp-millis"
	         },
	         "__originalname":"__change_time",
	         "__internal":true,
	         "__technical":true
	      },
	      {
	         "name":"__source_rowid",
	         "type":[
	            "null",
	            {
	               "type":"string",
	               "logicalType":"VARCHAR",
	               "length":30
	            }
	         ],
	         "__originalname":"__source_rowid",
	         "__internal":true,
	         "__primary_key_column":true,
	         "__technical":true
	      },
	      {
	         "name":"__source_transaction",
	         "type":[
	            "null",
	            {
	               "type":"string",
	               "logicalType":"VARCHAR",
	               "length":30
	            }
	         ],
	         "__originalname":"__source_transaction",
	         "__internal":true,
	         "__technical":true,
	         "__primary_key_column":true
	      },
	      {
	         "name":"__source_system",
	         "type":[
	            "null",
	            {
	               "type":"string",
	               "logicalType":"VARCHAR",
	               "length":30
	            }
	         ],
	         "__originalname":"__source_system",
	         "__internal":true,
	         "__technical":true,
	         "__primary_key_column":true
	      },
	      {
	         "name":"__extension",
	         "type":[
	            "null",
	            {
	               "type":"array",
	               "items":{
	                  "type":"record",
	                  "name":"__extension",
	                  "fields":[
	                     {
	                        "name":"__path",
	                        "type":{
	                           "type":"string",
	                           "logicalType":"STRING"
	                        },
	                        "__originalname":"__path"
	                     },
	                     {
	                        "name":"__value",
	                        "type":[
	                           "null",
	                           {
	                              "type":"boolean",
	                              "logicalType":"BOOLEAN"
	                           },
	                           {
	                              "type":"bytes",
	                              "logicalType":"BYTES"
	                           },
	                           {
	                              "type":"double",
	                              "logicalType":"DOUBLE"
	                           },
	                           {
	                              "type":"float",
	                              "logicalType":"FLOAT"
	                           },
	                           {
	                              "type":"int",
	                              "logicalType":"INT"
	                           },
	                           {
	                              "type":"long",
	                              "logicalType":"LONG"
	                           },
	                           {
	                              "type":"string",
	                              "logicalType":"STRING"
	                           }
	                        ],
	                        "__originalname":"__value"
	                     }
	                  ],
	                  "__originalname":"__extension"
	               }
	            }
	         ],
	         "default":null,
	         "__originalname":"__extension",
	         "__internal":true
	      },
	      {
	         "name":"__audit",
	         "type":[
	            "null",
	            {
	               "type":"record",
	               "name":"__audit",
	               "fields":[
	                  {
	                     "name":"__transformresult",
	                     "type":{
	                        "type":"string",
	                        "logicalType":"STRING"
	                     },
	                     "__originalname":"__transformresult"
	                  },
	                  {
	                     "name":"__details",
	                     "type":[
	                        "null",
	                        {
	                           "type":"array",
	                           "items":{
	                              "type":"record",
	                              "name":"__audit_details",
	                              "fields":[
	                                 {
	                                    "name":"__transformationname",
	                                    "type":{
	                                       "type":"string",
	                                       "logicalType":"STRING"
	                                    },
	                                    "__originalname":"__transformationname"
	                                 },
	                                 {
	                                    "name":"__transformresult",
	                                    "type":{
	                                       "type":"string",
	                                       "logicalType":"STRING"
	                                    },
	                                    "__originalname":"__transformresult"
	                                 },
	                                 {
	                                    "name":"__transformresult_text",
	                                    "type":[
	                                       "null",
	                                       {
	                                          "type":"string",
	                                          "logicalType":"STRING"
	                                       }
	                                    ],
	                                    "__originalname":"__transformresult_text"
	                                 },
	                                 {
	                                    "name":"__transformresult_quality",
	                                    "type":[
	                                       "null",
	                                       {
	                                          "type":"int",
	                                          "logicalType":"BYTE"
	                                       }
	                                    ],
	                                    "__originalname":"__transformresult_quality"
	                                 }
	                              ],
	                              "__originalname":"__audit_details"
	                           }
	                        }
	                     ],
	                     "default":null,
	                     "__originalname":"__details"
	                  }
	               ],
	               "__originalname":"__audit"
	            }
	         ],
	         "default":null,
	         "__originalname":"__audit",
	         "__internal":true
	      },
	      {
	         "name":"EmployeeNumber",
	         "type":{
	            "type":"string",
	            "logicalType":"VARCHAR",
	            "length":10
	         },
	         "__originalname":"EmployeeNumber",
	         "__primary_key_column":true
	      },
	      {
	         "name":"ChangeTimestamp",
	         "type":{
	            "type":"long",
	            "logicalType":"timestamp-millis"
	         },
	         "__originalname":"ChangeTimestamp",
	         "__primary_key_column":true
	      },
	      {
	         "name":"Firstname",
	         "type":[
	            "null",
	            {
	               "type":"string",
	               "logicalType":"NVARCHAR",
	               "length":40
	            }
	         ],
	         "__originalname":"Firstname"
	      },
	      {
	         "name":"Lastname",
	         "type":[
	            "null",
	            {
	               "type":"string",
	               "logicalType":"NVARCHAR",
	               "length":40
	            }
	         ],
	         "__originalname":"Lastname"
	      },
	      {
	         "name":"Title",
	         "type":[
	            "null",
	            {
	               "type":"string",
	               "logicalType":"NVARCHAR",
	               "length":20
	            }
	         ],
	         "__originalname":"Title"
	      },
	      {
	         "name":"CurrentDepartment",
	         "type":[
	            "null",
	            {
	               "type":"string",
	               "logicalType":"VARCHAR",
	               "length":20
	            }
	         ],
	         "__originalname":"CurrentDepartment"
	      },
	      {
	         "name":"CurrentPosition",
	         "type":[
	            "null",
	            {
	               "type":"string",
	               "logicalType":"VARCHAR",
	               "length":40
	            }
	         ],
	         "doc":"Current position",
	         "__originalname":"CurrentPosition"
	      },
	      {
	         "name":"EmployeeAddress",
	         "type":[
	            "null",
	            {
	               "type":"array",
	               "items":{
	                  "type":"record",
	                  "name":"EmployeeAddress",
	                  "fields":[
	                     {
	                        "name":"__extension",
	                        "type":[
	                           "null",
	                           {
	                              "type":"array",
	                              "items":"__extension"
	                           }
	                        ],
	                        "default":null,
	                        "__originalname":"__extension"
	                     },
	                     {
	                        "name":"AddressType",
	                        "type":[
	                           "null",
	                           {
	                              "type":"string",
	                              "logicalType":"VARCHAR",
	                              "length":10
	                           }
	                        ],
	                        "__originalname":"AddressType"
	                     },
	                     {
	                        "name":"City",
	                        "type":[
	                           "null",
	                           {
	                              "type":"string",
	                              "logicalType":"VARCHAR",
	                              "length":40
	                           }
	                        ],
	                        "__originalname":"City"
	                     },
	                     {
	                        "name":"Country",
	                        "type":[
	                           "null",
	                           {
	                              "type":"string",
	                              "logicalType":"NVARCHAR",
	                              "length":40
	                           }
	                        ],
	                        "__originalname":"Country"
	                     },
	                     {
	                        "name":"Street",
	                        "type":[
	                           "null",
	                           {
	                              "type":"string",
	                              "logicalType":"NVARCHAR",
	                              "length":40
	                           }
	                        ],
	                        "__originalname":"Street"
	                     },
	                     {
	                        "name":"PostCode",
	                        "type":[
	                           "null",
	                           {
	                              "type":"string",
	                              "logicalType":"VARCHAR",
	                              "length":10
	                           }
	                        ],
	                        "__originalname":"PostCode"
	                     }
	                  ],
	                  "__originalname":"EmployeeAddress"
	               }
	            }
	         ],
	         "default":null,
	         "__originalname":"EmployeeAddress"
	      }
	   ],
	   "__originalname":"Employee"
	}