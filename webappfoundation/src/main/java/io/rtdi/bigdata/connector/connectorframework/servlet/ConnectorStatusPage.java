package io.rtdi.bigdata.connector.connectorframework.servlet;

import jakarta.servlet.annotation.WebServlet;

@WebServlet("/ui5/ConnectorStatus")
public class ConnectorStatusPage extends UI5ServletAbstract {

	private static final long serialVersionUID = 122602943464363L;

	public ConnectorStatusPage() {
		super("Connector Status", "ConnectorStatus");
	}

}
