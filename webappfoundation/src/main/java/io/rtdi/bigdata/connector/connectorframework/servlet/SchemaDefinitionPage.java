package io.rtdi.bigdata.connector.connectorframework.servlet;

import jakarta.servlet.annotation.WebServlet;

@WebServlet("/ui5/SchemaDefinition")
public class SchemaDefinitionPage extends UI5ServletAbstract {

	private static final long serialVersionUID = 134293464363L;

	public SchemaDefinitionPage() {
		super("Schema Definition", "SchemaDefinition");
	}

}
