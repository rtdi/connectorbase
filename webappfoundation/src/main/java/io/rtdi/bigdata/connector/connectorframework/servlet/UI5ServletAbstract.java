package io.rtdi.bigdata.connector.connectorframework.servlet;

import java.io.IOException;
import java.io.PrintWriter;

import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.HttpConstraint;
import jakarta.servlet.annotation.ServletSecurity;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;

@ServletSecurity(@HttpConstraint(rolesAllowed = ServletSecurityConstants.ROLE_VIEW))
public abstract class UI5ServletAbstract extends HttpServlet {

	public static final int BROWSER_CACHING_IN_SECS = 1800;
	private static final long serialVersionUID = -5206967866294005565L;
	private String title;
	private String view;

	public UI5ServletAbstract(String title, String view) {
		this.title = title;
		this.view = view;
	}

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		long expiry = System.currentTimeMillis() + BROWSER_CACHING_IN_SECS*1000;
		response.setDateHeader("Expires", expiry);
		response.setHeader("Cache-Control", "max-age="+ BROWSER_CACHING_IN_SECS);
		ConnectorController connector = WebAppController.getConnectorOrFail(getServletContext());
		PrintWriter out = response.getWriter();
		out.println("<!DOCTYPE html>");
		out.println("<html style=\"height: 100%;\">");
		out.println("<head>");
		out.println("<meta charset=\"utf-8\"/>");
		out.print("<title>");
		out.print(title);
		out.println("</title>");
		out.println("<script src=\"" + connector.getGlobalSettings().getUi5url() + "\"");
		out.println("	id=\"sap-ui-bootstrap\""); 
		out.println("	data-sap-ui-theme=\"sap_fiori_3\"");
		out.println("	data-sap-ui-libs=\"sap.m\""); 
		out.println("	data-sap-ui-xx-bindingSyntax=\"complex\"");
		out.println("	data-sap-ui-resourceroots='{");
		out.println("        \"com.rtdi.bigdata.connector.ui\": \".\"");
		out.println("    }'");
		out.println("    data-sap-ui-async='true'>");
		out.println("</script>");

		out.println("<script>");
		out.println("	sap.ui.getCore().attachInit(function () {");
		out.println("		document.body.innerHTML=''");
		out.println("		new sap.ui.xmlview({");
		out.print("			viewName: \"com.rtdi.bigdata.connector.ui.view.");
		out.print(view);
		out.println("\"");
		out.println("		}).placeAt(\"content\");");
		out.println("	});");
		out.println("</script>");
		
		printCustomHeader(out);

		out.println("</head>");
		out.println("<body class='sapUiBody' id='content' style=\"height: 100%;\" >");
		out.println("<p>Loading OpenUI5 from <a href=\"" + connector.getGlobalSettings().getUi5url() + "\">here</a></p>");
		out.println("</body>");
		out.println("</html>");
	}

	protected void printCustomHeader(PrintWriter out) {
	}

	public String getTitle() {
		return title;
	}
}
