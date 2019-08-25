package io.rtdi.bigdata.connector.connectorframework.servlet;

import javax.servlet.annotation.WebServlet;

@WebServlet("/ui5/Services")
public class ServicesPage extends UI5ServletAbstract {

	private static final long serialVersionUID = 1226856464363L;

	public ServicesPage() {
		super("Services", "Services");
	}
}
