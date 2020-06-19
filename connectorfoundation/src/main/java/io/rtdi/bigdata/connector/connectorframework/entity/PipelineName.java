package io.rtdi.bigdata.connector.connectorframework.entity;

public class PipelineName {
	private String name;
	private String classname;
	
	public PipelineName(String name, String classname) {
		this.name = name;
		this.classname = classname;
	}
	
	public String getName() {
		return name;
	}
	public String getClassname() {
		return classname;
	}
}
