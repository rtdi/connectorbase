package io.rtdi.bigdata.connector.pipeline.foundation;

import java.io.IOException;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.OperationLogContainer;
import io.rtdi.bigdata.kafka.avro.RuleResult;

public abstract class MicroServiceTransformation implements Comparable<MicroServiceTransformation> {
	
	private String name;
	private long rowsprocessed = 0;
	private Long lastRowProcessed = null;
	protected OperationLogContainer states = new OperationLogContainer();
	protected final Logger logger;

	public MicroServiceTransformation(String name) {
		this.name = Objects.requireNonNull(name, "No name for the microservice provided");
		logger = LogManager.getLogger(this.getClass().getName());
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

	@Override
	public String toString() {
		return "Microservice Transformation " + name;
	}

}
