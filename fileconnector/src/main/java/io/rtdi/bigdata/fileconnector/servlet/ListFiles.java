package io.rtdi.bigdata.fileconnector.servlet;

import javax.servlet.annotation.WebServlet;

import io.rtdi.bigdata.connector.connectorframework.servlet.UI5ServletAbstract;

/**
 * Servlet implementation class ListFiles
 */
@WebServlet("/ui5/ListFiles")
public class ListFiles extends UI5ServletAbstract {
	private static final long serialVersionUID = 1L;

    public ListFiles() {
        super("List files", "ListFiles");
    }

}
