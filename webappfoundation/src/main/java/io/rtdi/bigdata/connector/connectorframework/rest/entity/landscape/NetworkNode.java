package io.rtdi.bigdata.connector.connectorframework.rest.entity.landscape;

import java.util.ArrayList;
import java.util.List;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesRuntimeException;

public class NetworkNode {
	private String id;
	private String label;
	private String title;
	private String type;
	private List<NetworkNode> children;
	private List<String> links;

	public NetworkNode() {
	}
	
	public NetworkNode(String id, NetworkNodeType type, String label, String desc) throws PropertiesRuntimeException {
		if (id == null || label == null || type == null) {
			throw new PropertiesRuntimeException("Neither of id, label or type can be null (\"" +
					id + "\"," +
					label + "\"," +
					type + "\")" 
					);
		}
		this.id = id;
		this.label = label;
		this.title = desc;
		this.type = type.name();
	}
	
	public void addChild(NetworkNode n) {
		if (children == null) {
			children = new ArrayList<>();
		}
		children.add(n);
	}
	
	public void addLink(NetworkNode n) {
		if (links == null) {
			links = new ArrayList<>();
		}
		links.add(n.getId());
	}

	public void addLink(String id) {
		if (links == null) {
			links = new ArrayList<>();
		}
		links.add(id);
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

	public List<NetworkNode> getChildren() {
		return children;
	}

	public List<String> getLinks() {
		return links;
	}

}
