package io.rtdi.bigdata.connector.pipeline.foundation;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;

public interface IRecordMapping {

	JexlRecord apply(JexlRecord input);

	String getName();

	Schema getOutputSchema();

	SchemaHandler getOutputSchemaHandler();

}