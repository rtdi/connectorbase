package io.rtdi.bigdata.connector.connectorframework.servlet;

import jakarta.servlet.annotation.WebServlet;

@WebServlet("/ui5/Topics")
public class TopicsPage extends UI5ServletAbstract {

	private static final long serialVersionUID = 1342943464363L;

	public TopicsPage() {
		super("Topics", "Topics");
	}

}
