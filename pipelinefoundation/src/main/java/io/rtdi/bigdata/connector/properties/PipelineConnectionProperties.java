package io.rtdi.bigdata.connector.properties;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class PipelineConnectionProperties extends PipelineConnectionServerProperties {
	private String tenantid = null;
	private static final String TENANTID = "tenantid";

	public PipelineConnectionProperties(String name) {
		super(name);
		properties.addStringProperty(TENANTID, "Tenant id", "An arbitary name for the tenant", null, null, true);
	}

	public String getTenantid() {
		if (tenantid == null) {
			tenantid = properties.getStringPropertyValue(TENANTID);
		}
		return tenantid;
	}

	public void setTenantid(String tenantid) throws PropertiesException {
		properties.setProperty(TENANTID, tenantid);
		this.tenantid = tenantid;
	}

}
