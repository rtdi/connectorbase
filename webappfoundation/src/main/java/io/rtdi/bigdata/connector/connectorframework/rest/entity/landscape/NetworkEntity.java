package io.rtdi.bigdata.connector.connectorframework.rest.entity.landscape;

import java.util.ArrayList;
import java.util.Hashtable;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesRuntimeException;

public class NetworkEntity {

	protected ArrayList<NetworkNode> nodes;
	protected ArrayList<NetworkEdge> edges;
	protected Hashtable<String, Integer> nodeindex;
	protected Hashtable<String, Integer> edgeindex;

	public NetworkEntity() {
		super();
		nodes = new ArrayList<NetworkNode>();
		edges = new ArrayList<NetworkEdge>();
		nodeindex = new Hashtable<String, Integer>();
		edgeindex = new Hashtable<String, Integer>();
	}

	public ArrayList<NetworkNode> getNodes() {
		return nodes;
	}

	public void setNodes(ArrayList<NetworkNode> nodes) {
		this.nodes = nodes;
		if (nodes != null) {
			for (int i=0; i<nodes.size(); i++) {
				nodeindex.put(nodes.get(i).getId(), i);
			}
		}
	}

	public void setEdges(ArrayList<NetworkEdge> edges) {
		this.edges = edges;
		if (edges != null) {
			for (int i=0; i<edges.size(); i++) {
				String id = edges.get(i).getFrom() + "_" + edges.get(i).getTo();
				edgeindex.put(id, i);
			}
		}
	}

	public ArrayList<NetworkEdge> getEdges() {
		return edges;
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
		b.append("]\r\nEdges: [\r\n");
		for (NetworkEdge edge: edges) {
			b.append("");
			b.append(edge.toString());
			b.append("\r\n");
		}
		b.append("]");
		return b.toString();
	}

	public void addNode(String id, NetworkNodeType type, String label, String desc) throws PropertiesRuntimeException {
		Integer index = nodeindex.get(id);
		if (index == null) {
			index = nodes.size();
			nodes.add(new NetworkNode(id, type, label, desc));
			nodeindex.put(id, index);
		} // else node exists already
	}

	public void addEdge(String from, String to) throws PropertiesRuntimeException {
		String id = from + "_" + to;
		Integer index = edgeindex.get(id);
		if (index == null) {
			index = edges.size();
			edges.add(new NetworkEdge(from, to));
			edgeindex.put(id, index);
		} // else Edge exists already
	}

	public void removeNode(String id) {
		Integer index = nodeindex.get(id);
		if (index != null) {
			nodes.remove(index.intValue());
			nodeindex.remove(id);
			removeAllEdges(id);
		}
	}

	public void removeAllEdges(String id) {
		String from = id + "_";
		String to = "_" + id;
		for(String edgeid : edgeindex.keySet()) {
			if (edgeid.startsWith(from) || edgeid.endsWith(to)) {
				Integer index = edgeindex.get(edgeid);
				edges.remove(index.intValue());
				edgeindex.remove(edgeid);
			}
		}
	}

}