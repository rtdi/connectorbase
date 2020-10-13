package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Epoch {
	private long epoch;

	public Epoch() {
	}

	public Epoch(long epoch) {
		this.epoch = epoch;
	}

	public long getEpoch() {
		return epoch;
	}

	public void setEpoch(long epoch) {
		this.epoch = epoch;
	}
	
}
