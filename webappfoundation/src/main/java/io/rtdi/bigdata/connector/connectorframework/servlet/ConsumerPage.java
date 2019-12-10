package io.rtdi.bigdata.connector.connectorframework.servlet;

import javax.servlet.annotation.WebServlet;

@WebServlet("/ui5/Consumer")
public class ConsumerPage extends UI5ServletAbstract {

	private static final long serialVersionUID = 122074464363L;

	public ConsumerPage() {
		super("Consumer", "Consumer");
	}
}
