package io.rtdi.bigdata.connector.pipeline.foundation.enums;

/**
 * A single consumer instance can have one of these states
 *
 */
public enum OperationState {
	/**
	 * The instance is stopped, no thread running.
	 */
	STOPPED,
	/**
	 * It is has requested data from the server and will fetch the data next. Example would be a http call being made to the server.
	 */
	REQUESTDATA,
	/**
	 * The data has been fetched and the request at the server returned
	 */
	DONEREQUESTDATA,
	/**
	 * The connection with the server is open and a PipelineAPI.fetch call has been invoked
	 */
	FETCH,
	/**
	 * The fetch has been issued but no data received yet - maybe there is none and the process is waiting
	 */
	FETCHWAITINGFORDATA,
	/**
	 * The fetch got a record and it is being processed at the moment
	 */
	FETCHGETTINGROW,
	/**
	 * Something severe happened, all connections to the source/target/server are dropped and will be reestablished from ground on after a grace period
	 */
	ERRORWAITFORRETRY,
	/**
	 * The fetch completed
	 */
	DONEFETCH,
	/**
	 * In the process to flush the data
	 */
	DOEXPLICITCOMMIT,
	/**
	 * FLushing the data to the persistence is completed
	 */
	DONEEXPLICITCOMMIT,
	/**
	 * Currently closing the connections
	 */
	CLOSE,
	/**
	 * Connections all closed
	 */
	DONECLOSE,
	/**
	 * Open the connections
	 */
	OPEN,
	/**
	 * All connections are open
	 */
	DONEOPEN,
	/**
	 * Caught Error, try again
	 */
	RECOVERABLEERROR,
	/**
	 * Caught Error on PipelineAPI level
	 */
	APIERROR
	
}
