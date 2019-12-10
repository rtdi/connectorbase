package io.rtdi.bigdata.connector.pipeline.foundation.entity;

public class JAXBSuccessMessage {
	private String text;
			
	public JAXBSuccessMessage() {
		super();
	}
	
	public JAXBSuccessMessage(String text) {
		super();
		this.text = text;
	}

	public String getText() {
		return text;
	}
	
	public void setText(String text) {
		this.text = text;
	}

}
