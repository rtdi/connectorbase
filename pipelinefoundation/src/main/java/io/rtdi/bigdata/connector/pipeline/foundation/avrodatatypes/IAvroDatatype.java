package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineCallerException;

public interface IAvroDatatype {

	void toString(StringBuffer b, Object value);

	Object convertToInternal(Object value) throws PipelineCallerException;

	Type getBackingType();

	Schema getDatatypeSchema();

	AvroType getAvroType();
	
	Object convertToJava(Object value) throws PipelineCallerException;
}
