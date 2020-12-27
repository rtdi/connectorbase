package io.rtdi.bigdata.connector.connectorframework.servlet;

import jakarta.servlet.annotation.WebServlet;

@WebServlet("/ui5/Connection")
public class ConnectionPage extends UI5ServletAbstract {

	private static final long serialVersionUID = 122602943464363L;

	public ConnectionPage() {
		super("Connection", "Connection");
	}
}
