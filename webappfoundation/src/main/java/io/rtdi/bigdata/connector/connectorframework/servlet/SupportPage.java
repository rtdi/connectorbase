package io.rtdi.bigdata.connector.connectorframework.servlet;

import javax.servlet.annotation.WebServlet;

@WebServlet("/ui5/Support")
public class SupportPage extends UI5ServletAbstract {

	private static final long serialVersionUID = 1294563469L;

	public SupportPage() {
		super("Support", "Support");
	}
}
