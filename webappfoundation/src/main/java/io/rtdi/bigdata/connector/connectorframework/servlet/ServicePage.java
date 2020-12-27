package io.rtdi.bigdata.connector.connectorframework.servlet;

import jakarta.servlet.annotation.WebServlet;

@WebServlet("/ui5/Service")
public class ServicePage extends UI5ServletAbstract {

	private static final long serialVersionUID = 143523464363L;

	public ServicePage() {
		super("Service", "Service");
	}
}
