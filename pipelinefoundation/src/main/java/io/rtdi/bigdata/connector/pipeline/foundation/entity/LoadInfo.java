package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import io.rtdi.bigdata.connector.pipeline.foundation.PipelineAbstract;

public class LoadInfo {

	private String producername;
	private String schemaname;
	private int producerinstanceno;
	private String transactionid;
	private Long completiontime;
	private Long rowcount;
	private boolean resetdelta = false;

	public LoadInfo() {
		super();
	}
	
	public LoadInfo(String producername, String schemaname, int producerinstanceno, String transactionid, Long completiontime, Long rowcount) {
		this.producername = producername;
		this.schemaname = schemaname;
		this.producerinstanceno = producerinstanceno;
		this.transactionid = transactionid;
		this.completiontime = completiontime;
	}
	
	public String getProducername() {
		return producername;
	}
	public String getSchemaname() {
		return schemaname;
	}
	public int getProducerinstanceno() {
		return producerinstanceno;
	}
	public String getTransactionid() {
		return transactionid;
	}
	public Long getCompletiontime() {
		return completiontime;
	}
	public Long getRowcount() {
		return rowcount;
	}

	public void setProducername(String producername) {
		this.producername = producername;
	}

	public void setSchemaname(String schemaname) {
		this.schemaname = schemaname;
	}

	public void setProducerinstanceno(int producerinstanceno) {
		this.producerinstanceno = producerinstanceno;
	}

	public void setTransactionid(String transactionid) {
		this.transactionid = transactionid;
	}

	public void setCompletiontime(Long completiontime) {
		this.completiontime = completiontime;
	}

	public void setRowcount(Long rowcount) {
		this.rowcount = rowcount;
	}

	public boolean isResetdelta() {
		return resetdelta;
	}

	public void setResetdelta(boolean resetdelta) {
		this.resetdelta = resetdelta;
	}
	
	@Override
	public String toString() {
		if (schemaname != null && schemaname.equals(PipelineAbstract.ALL_SCHEMAS)) {
			return "DeltaIteration for Producer: " + producername + " completed with transaction: " + transactionid;
		} else {
			return "Initialload for Producer: " + producername + ", schemaname: " + schemaname + " completed with transaction: " + transactionid;
		}
	}
}
