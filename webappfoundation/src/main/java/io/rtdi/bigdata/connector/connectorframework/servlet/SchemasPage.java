package io.rtdi.bigdata.connector.connectorframework.servlet;

import javax.servlet.annotation.WebServlet;

@WebServlet("/ui5/Schemas")
public class SchemasPage extends UI5ServletAbstract {

	private static final long serialVersionUID = 1342943464363L;

	public SchemasPage() {
		super("Schemas", "Schemas");
	}

}
