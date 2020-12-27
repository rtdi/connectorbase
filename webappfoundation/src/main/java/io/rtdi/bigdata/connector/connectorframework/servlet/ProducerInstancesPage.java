package io.rtdi.bigdata.connector.connectorframework.servlet;

import jakarta.servlet.annotation.WebServlet;

@WebServlet("/ui5/ProducerInstances")
public class ProducerInstancesPage extends UI5ServletAbstract {

	private static final long serialVersionUID = 609543363L;

	public ProducerInstancesPage() {
		super("ProducerInstances", "ProducerInstances");
	}
}
