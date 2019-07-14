package io.rtdi.bigdata.fileconnector;

import io.rtdi.bigdata.connector.connectorframework.JerseyApplication;

public class JerseyApplicationFileConnector extends JerseyApplication {

	public JerseyApplicationFileConnector() {
		super();
	}

	@Override
	protected String[] getPackages() {
		return new String[] {"io.rtdi.bigdata.fileconnector.service"};
	}

}
