package io.rtdi.bigdata.connector.connectorframework.rest.entity.landscape;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesRuntimeException;

public class NetworkEntity {

	protected List<NetworkNode> nodes;
	protected Map<String, NetworkNode> nodeindex;

	public NetworkEntity() {
		super();
		nodes = new ArrayList<NetworkNode>();
		nodeindex = new HashMap<>();
	}

	public List<NetworkNode> getNodes() {
		return nodes;
	}

	@Override
	public String toString() {
		StringBuffer b = new StringBuffer();
		b.append("Nodes: [\r\n");
		for (NetworkNode node : nodes) {
			b.append("");
			b.append(node.toString());
			b.append("\r\n");
		}
		b.append("]");
		return b.toString();
	}

	public NetworkNode addNode(String id, NetworkNodeType type, String label, String desc) throws PropertiesRuntimeException {
		NetworkNode n = nodeindex.get(id);
		if (n == null) {
			n = new NetworkNode(id, type, label, desc);
			nodes.add(n);
			nodeindex.put(id, n);
		}
		return n;
	}

	public void removeNode(NetworkNode n) {
		nodes.remove(n);
	}

}