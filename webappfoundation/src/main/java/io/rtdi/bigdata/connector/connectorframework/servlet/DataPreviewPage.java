package io.rtdi.bigdata.connector.connectorframework.servlet;

import javax.servlet.annotation.WebServlet;

@WebServlet("/ui5/DataPreview")
public class DataPreviewPage extends UI5ServletAbstract {

	private static final long serialVersionUID = 192943464363L;

	public DataPreviewPage() {
		super("DataPreview", "DataPreview");
	}

}
