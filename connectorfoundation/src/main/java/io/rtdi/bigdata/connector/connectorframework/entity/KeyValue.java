package io.rtdi.bigdata.connector.connectorframework.entity;

public class KeyValue {
	private String key;
	private String value;
	
	public KeyValue() {
		super();
	}

	public KeyValue(String value) {
		this(value, value);
	}

	public KeyValue(String key, String value) {
		super();
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	
	
}
