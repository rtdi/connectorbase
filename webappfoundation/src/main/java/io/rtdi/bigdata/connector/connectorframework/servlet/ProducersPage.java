package io.rtdi.bigdata.connector.connectorframework.servlet;

import javax.servlet.annotation.WebServlet;

@WebServlet("/ui5/Producers")
public class ProducersPage extends UI5ServletAbstract {

	private static final long serialVersionUID = 12260943464363L;

	public ProducersPage() {
		super("Producers", "Producers");
	}
}
