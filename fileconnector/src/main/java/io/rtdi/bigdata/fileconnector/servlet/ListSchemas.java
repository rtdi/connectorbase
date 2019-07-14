package io.rtdi.bigdata.fileconnector.servlet;

import javax.servlet.annotation.WebServlet;

import io.rtdi.bigdata.connector.connectorframework.servlet.UI5ServletAbstract;

/**
 * Servlet implementation class ListFiles
 */
@WebServlet("/ui5/ListSchemas")
public class ListSchemas extends UI5ServletAbstract {
	private static final long serialVersionUID = 1L;

    public ListSchemas() {
        super("List Schemas", "ListSchemas");
    }

}
