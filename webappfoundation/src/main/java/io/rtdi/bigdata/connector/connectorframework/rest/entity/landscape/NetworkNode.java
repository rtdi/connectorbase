package io.rtdi.bigdata.connector.connectorframework.rest.entity.landscape;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesRuntimeException;

public class NetworkNode {
	private String id;
	private String label;
	private String title;
	private String type;

	public NetworkNode() {
	}
	
	public NetworkNode(String id, NetworkNodeType type, String label, String desc) throws PropertiesRuntimeException {
		if (id == null || label == null || desc == null) {
			throw new PropertiesRuntimeException("Neither of id, label, desc or color can be null (\"" +
					id + "\"," +
					label + "\"," +
					desc + "\")" 
					);
		}
		this.id = id;
		this.label = label;
		this.title = desc;
		this.type = type.name();
	}

	public String getId() {
		return id;
	}

	public String getLabel() {
		return label;
	}

	public String getTitle() {
		return title;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public void setTitle(String title) {
		this.title = title;
	}


	@Override
	public int hashCode() {
		return id.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		} else if (obj instanceof NetworkNode) {
			return id.equals(((NetworkNode) obj).getId());
		} else {
			return id.equals(obj);
		}
	}

	@Override
	public String toString() {
		return id;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setType(NetworkNodeType type) {
		this.type = type.name();
	}
}
