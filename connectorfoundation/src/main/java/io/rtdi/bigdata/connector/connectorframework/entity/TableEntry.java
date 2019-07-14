package io.rtdi.bigdata.connector.connectorframework.entity;

import io.rtdi.bigdata.connector.pipeline.foundation.enums.TableType;

public class TableEntry {
	private String tablename;
	private TableType tabletype;
	private String description;
	
	public TableEntry() {
		this(null, null, null);
	}
	
	public TableEntry(String tablename, TableType tabletype, String description) {
		super();
		this.tablename = tablename;
		this.tabletype = tabletype;
		this.description = description;
	}
	
	public TableEntry(String tablename) {
		this(tablename, TableType.TABLE, null);
	}
		
	public String getTablename() {
		return tablename;
	}
	
	public void setTablename(String tablename) {
		this.tablename = tablename;
	}
	
	public String getTabletype() {
		return tabletype.name();
	}
	
	public void setTabletype(String tabletype) {
		this.tabletype = TableType.valueOf(tabletype);
	}
	
	public String getDescription() {
		return description;
	}
	
	public void setDescription(String description) {
		this.description = description;
	}

	@Override
	public String toString() {
		return tablename;
	}
	
}
