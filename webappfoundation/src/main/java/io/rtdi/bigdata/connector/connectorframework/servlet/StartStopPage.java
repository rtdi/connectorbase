package io.rtdi.bigdata.connector.connectorframework.servlet;

import jakarta.servlet.annotation.WebServlet;

@WebServlet("/ui5/StartStop")
public class StartStopPage extends UI5ServletAbstract {

	private static final long serialVersionUID = 12965474563469L;

	public StartStopPage() {
		super("StartStop", "StartStop");
	}
}
