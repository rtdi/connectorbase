package io.rtdi.bigdata.connector.pipeline.foundation;

import java.io.IOException;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.OperationLogContainer;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RuleResult;

public abstract class MicroServiceTransformation implements Comparable<MicroServiceTransformation> {
	
	private String name;
	private long rowsprocessed = 0;
	private Long lastRowProcessed = null;
	protected OperationLogContainer states = new OperationLogContainer();

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
	
	@Override
	public boolean equals(Object o) {
		return name.equals(o);
	}
	
	@Override
	public int hashCode() {
		return name.hashCode();
	}

	@Override
	public int compareTo(MicroServiceTransformation o) {
		return name.compareTo(o.getName());
	}
	
	/**
	 * Allows to append a information to the state display element of type PASS (=Normal)
	 * 
	 * @param text to add
	 */
	public void addOperationLogLine(String text) {
		states.add(text);
	}

	/**
	 * Allows to append a information to the state display element
	 * 
	 * @param text to add
	 * @param description free form text
	 * @param state if it is a normal entry, a warning or something severe
	 */
	public void addOperationLogLine(String text, String description, RuleResult state) {
		states.add(text, description, state);
	}

	/**
	 * @return the last n state texts
	 */
	public OperationLogContainer getOperationLog() {
		return states;
	}

}
