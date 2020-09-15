package io.rtdi.bigdata.connector.pipeline.foundation.avro;

import org.apache.avro.Schema.Field;

public interface IAvroNested {

	public IAvroNested getParent();
	
	public void setParent(IAvroNested parent);
	
	String getPath();
	
	Field getParentField();
}
