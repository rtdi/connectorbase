package io.rtdi.bigdata.connector.connectorframework.servlet;

import jakarta.servlet.annotation.WebServlet;

@WebServlet("/ui5/Home")
public class HomePage extends UI5ServletAbstract {

	private static final long serialVersionUID = 12260296578464363L;

	public HomePage() {
		super("Home", "Home");
	}

}
