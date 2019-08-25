package io.rtdi.bigdata.connector.pipeline.foundation;

import java.io.IOException;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;

public abstract class MicroServiceTransformation {
	
	private String name;
	private long rowsprocessed = 0;
	private Long lastRowProcessed = null;

	public MicroServiceTransformation(String name) {
		this.name = name;
	}

	public final JexlRecord apply(JexlRecord valuerecord) throws IOException {
		JexlRecord result = applyImpl(valuerecord);
		rowsprocessed++;
		lastRowProcessed = System.currentTimeMillis();
		return result;
	}
	
	public abstract JexlRecord applyImpl(JexlRecord valuerecord) throws IOException;

	public long getRowProcessed() {
		return rowsprocessed;
	}
	
	public Long getLastRowProcessed() {
		return lastRowProcessed;
	}

	public String getName() {
		return name;
	}
}
