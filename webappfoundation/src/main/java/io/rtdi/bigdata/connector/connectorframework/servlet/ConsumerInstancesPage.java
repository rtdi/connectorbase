package io.rtdi.bigdata.connector.connectorframework.servlet;

import javax.servlet.annotation.WebServlet;

@WebServlet("/ui5/ConsumerInstances")
public class ConsumerInstancesPage extends UI5ServletAbstract {

	private static final long serialVersionUID = 122609543363L;

	public ConsumerInstancesPage() {
		super("ConsumerInstances", "ConsumerInstances");
	}
}
