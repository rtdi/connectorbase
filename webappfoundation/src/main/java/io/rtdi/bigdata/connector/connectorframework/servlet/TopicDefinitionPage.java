package io.rtdi.bigdata.connector.connectorframework.servlet;

import javax.servlet.annotation.WebServlet;

@WebServlet("/ui5/TopicDefinition")
public class TopicDefinitionPage extends UI5ServletAbstract {

	private static final long serialVersionUID = 1342943464363L;

	public TopicDefinitionPage() {
		super("Topic Definition", "TopicDefinition");
	}

}
