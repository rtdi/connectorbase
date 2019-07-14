package io.rtdi.bigdata.connector.connectorframework.servlet;

import javax.servlet.annotation.WebServlet;

@WebServlet("/ui5/PipelineConnection")
public class PipelineConnectionPage extends UI5ServletAbstract {

	private static final long serialVersionUID = 1226029476483073544L;

	public PipelineConnectionPage() {
		super("Pipeline Connection", "PipelineConnection");
	}

}
