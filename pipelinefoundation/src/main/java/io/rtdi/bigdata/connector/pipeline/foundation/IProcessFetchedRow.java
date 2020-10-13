package io.rtdi.bigdata.connector.pipeline.foundation;

import java.io.IOException;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.OperationState;

public interface IProcessFetchedRow {
	
	void process(TopicName topic, long offset, long offsettimestamp, int partition, JexlRecord keyRecord, JexlRecord valueRecord) throws IOException;
	
	void setOperationState(OperationState state);

	/**
	 * @return true if the consumer should continue reading data - used to exit processing the poll returned rowset early 
	 */
	boolean isActive();

	/**
	 * Method to update the consumers informational metadata
	 * 
	 * @param offset of the processed message
	 * @param offsettimestamp is the timestamp related to the message offset
	 */
	void incrementRowsProcessed(long offset, long offsettimestamp);

}
