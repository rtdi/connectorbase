package io.rtdi.bigdata.connector.connectorframework.servlet;

import jakarta.servlet.annotation.WebServlet;

@WebServlet("/ui5/Consumers")
public class ConsumersPage extends UI5ServletAbstract {

	private static final long serialVersionUID = 12260943464363L;

	public ConsumersPage() {
		super("Consumers", "Consumers");
	}
}
