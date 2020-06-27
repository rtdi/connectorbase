package io.rtdi.bigdata.connector.pipeline.foundation.entity;

public class JAXBErrorMessage extends ErrorEntity {
	
	public JAXBErrorMessage(Throwable e) {
		super(e);
	}
		
	public JAXBErrorMessage() {
		super();
	}
	
	public JAXBErrorMessage(String errortext) {
		super();
		this.message = errortext;
	}

}
