package io.rtdi.bigdata.connector.connectorframework.servlet;

import jakarta.servlet.annotation.WebServlet;

@WebServlet("/ui5/ServiceStatistics")
public class ServiceStatisticsPage extends UI5ServletAbstract {

	private static final long serialVersionUID = 34634643;

	public ServiceStatisticsPage() {
		super("ServiceStatistics", "ServiceStatistics");
	}
}
