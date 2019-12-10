package io.rtdi.bigdata.connector.connectorframework.rest.entity.landscape;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesRuntimeException;

public class NetworkEdge {
	private String from;
	private String to;

	public NetworkEdge() {
	}
	
	public NetworkEdge(String fromid, String toid) throws PropertiesRuntimeException {
		if (fromid == null || toid == null) {
			throw new PropertiesRuntimeException("Neither from nor to can be null (\"" + from + "\", \"" + to + "\")");
		}
		this.from = fromid;
		this.to = toid;
	}

	public String getFrom() {
		return from;
	}

	public String getTo() {
		return to;
	}

	public void setFrom(String from) {
		this.from = from;
	}

	public void setTo(String to) {
		this.to = to;
	}

	@Override
	public String toString() {
		return from + "-->" + to;
	}

}
