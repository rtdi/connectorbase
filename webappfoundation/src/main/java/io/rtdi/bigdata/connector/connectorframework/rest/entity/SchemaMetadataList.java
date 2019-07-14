package io.rtdi.bigdata.connector.connectorframework.rest.entity;

import java.util.List;

import io.rtdi.bigdata.connector.connectorframework.entity.TableEntry;

public class SchemaMetadataList {
	private List<TableEntry> tablenames;

	public SchemaMetadataList() {
		super();
	}
	
	public SchemaMetadataList(List<TableEntry> schemas) {
		super();
		this.tablenames = schemas;
	}

	public List<TableEntry> getTablenames() {
		return tablenames;
	}

	public void setTablenames(List<TableEntry> tablenames) {
		this.tablenames = tablenames;
	}
	

}
