package io.rtdi.bigdata.connector.pipeline.foundation.avro;

public interface IAvroNested {

	public IAvroNested getParent();
	
	public void setParent(IAvroNested parent);
	
	String getPath();
}
