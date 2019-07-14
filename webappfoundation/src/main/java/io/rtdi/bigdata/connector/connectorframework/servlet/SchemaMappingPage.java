package io.rtdi.bigdata.connector.connectorframework.servlet;

import javax.servlet.annotation.WebServlet;

@WebServlet("/ui5/SchemaMapping")
public class SchemaMappingPage extends UI5ServletAbstract {

	private static final long serialVersionUID = 134293565363L;

	public SchemaMappingPage() {
		super("Schema Mapping", "SchemaMapping");
	}

}
