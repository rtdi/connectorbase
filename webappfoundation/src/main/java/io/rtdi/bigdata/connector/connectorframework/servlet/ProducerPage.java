package io.rtdi.bigdata.connector.connectorframework.servlet;

import javax.servlet.annotation.WebServlet;

@WebServlet("/ui5/Producer")
public class ProducerPage extends UI5ServletAbstract {

	private static final long serialVersionUID = 122602943464963L;

	public ProducerPage() {
		super("Producer", "Producer");
	}
}
