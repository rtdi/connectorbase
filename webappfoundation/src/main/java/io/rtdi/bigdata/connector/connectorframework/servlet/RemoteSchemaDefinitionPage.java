package io.rtdi.bigdata.connector.connectorframework.servlet;

import javax.servlet.annotation.WebServlet;

@WebServlet("/ui5/RemoteSchemaDefinition")
public class RemoteSchemaDefinitionPage extends UI5ServletAbstract {

	private static final long serialVersionUID = 134293464363L;

	public RemoteSchemaDefinitionPage() {
		super("Remote Schema Definition", "RemoteSchemaDefinition");
	}

}
