## Thread Hierarchy

* ServletContextListener: Is the root process and starting point to control all threads.
  * ConnectorController: Governs the PipelineAPI connection and starts/stops/reloads all child processes, e.g. when the PipelineAPI properties file does change or a connection is added/removed
    * ConnectionController: A thread that is responsible to handle all state changes of the connection, e.g. change in the connection properties, connection failed, adding producers/consumers
      * ProducerController: A thread controlling the instances of a producer
        * ProducerInstanceController: Does the actual reading from the source and writing into the pipeline
      * ConsumerController: A thread controlling all instances of a consumer
        * ConsumerInstanceController: Does the actual fetching from the pipeline and writes into the target



## Exception Hierarchy

* PropertiesException: The configuration is totally wrong, it makes no sense to continue. Used when reading the configurations but also when calling methods with invalid arguments
  * ConnectorException: There is something severely wrong with the Connector, stop. Essentially this is an exception with a hint and a causing object information.
    * ConnectorRuntimeException: During the normal operation a non-recoverable problem occured
      * ConnectorTemporaryException: Shutdown all connections and restart again
        * ConnectorCallerException: An external party called with wrong parameters, not our fault
    * PipelineRuntimeException: During the normal operation with the pipeline an non-recoverable problem occured
      * PipelineTemporaryException: Shutdown the pipeline and restart
        * PipelineCallerException: An external party called with wrong parameters, not our fault



ProducerInstanceController is using the Producer to fetch records and sends them via the ProducerSession.

ConsumerInstanceController is using the ConsumerSession to fetch data and send it to Consumer for writing.

Pipeline does not cache any data like topics or schemas, the ProducerInstanceController does and the ConsumerSession.



