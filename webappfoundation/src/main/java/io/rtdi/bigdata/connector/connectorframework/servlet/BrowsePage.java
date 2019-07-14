package io.rtdi.bigdata.connector.connectorframework.servlet;

import javax.servlet.annotation.*;

@WebServlet("/ui5/Browse")
public class BrowsePage extends UI5ServletAbstract {

	private static final long serialVersionUID = 1342943464363L;

	public BrowsePage() {
		super("Browse", "Browse");
	}

}
