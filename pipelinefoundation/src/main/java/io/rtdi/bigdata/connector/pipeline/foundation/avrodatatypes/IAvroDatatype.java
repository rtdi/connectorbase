package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.Schema.Type;

public interface IAvroDatatype {

	void toString(StringBuffer b, Object value);

	Object convertToInternal(Object value);

	Type getBackingType();

}
