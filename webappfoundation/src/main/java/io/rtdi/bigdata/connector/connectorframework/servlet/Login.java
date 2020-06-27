package io.rtdi.bigdata.connector.connectorframework.servlet;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.IOUtils;

@WebServlet("/login")
public class Login extends HttpServlet {

	private static final long serialVersionUID = 454601115219128919L;

	public Login() {
		super();
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		PrintWriter out = resp.getWriter();
		HttpSession session = req.getSession(false);
		String username = null;
		String password = null;
		if (session != null) {
			username = (String) session.getAttribute("username");
			password = (String) session.getAttribute("password");
			session.removeAttribute("username");
			session.removeAttribute("password");
			if (username != null && password != null) {
				String target = "j_security_check?j_username=" + IOUtils.urlEncode(username) + "&j_password=" + IOUtils.urlEncode(password);
				resp.sendRedirect(target);
				out.println("<!DOCTYPE html>");
				out.println("<html><head></head><body>");
				out.println("Redirecting to the <a href=\"" + target + "\">tomcat authenticator</a> (j_security_check)</body></html>");
				return;
			}
		}
		if (username == null) username = "";
		if (password == null) password = "";
		ConnectorController connector = WebAppController.getConnector(getServletContext());
		String ui5url;
		String helpurl;
		if (connector != null) {
			ui5url = connector.getGlobalSettings().getUi5url();
			helpurl = connector.getGlobalSettings().getConnectorHelpURL();
		} else {
			ui5url = "https://openui5.hana.ondemand.com/resources/sap-ui-core.js";
			helpurl = null;
		}
		
		out.println("<!DOCTYPE html>");
		out.println("<html style=\"height: 100%;\">");
		out.println("<head>");
		out.println("<meta charset=\"ISO-8859-1\">");
		out.println("<title>Login</title>");
		out.println("<script src=\"" + ui5url + "\"");
		out.println("	id=\"sap-ui-bootstrap\""); 
		out.println("	data-sap-ui-theme=\"sap_fiori_3\"");
		out.println("	data-sap-ui-libs=\"sap.m\">");
		out.println("</script>");

		out.println("<script id=\"view1\" type=\"sapui5/xmlview\">");
		out.println("    <mvc:View");
		out.println("        controllerName=\"local.controller\"");
		out.println("        xmlns:mvc=\"sap.ui.core.mvc\"");
		out.println("        xmlns=\"sap.m\">");
		out.println("        <App>");
		out.println("            <Page title=\"Login Form\">");
		out.println("				<content>");
		out.println("					<VBox fitContainer=\"true\" justifyContent=\"Center\" alignItems=\"Center\" alignContent=\"Center\">");
		out.println("						<items>");
		if (helpurl != null) {
			out.println("						    <Link text=\"HELP Information\" target=\"_blank\" href=\"" + helpurl + "\" />");
		}
		out.println("							<Input id=\"uid\" name=\"j_username\" value=\"" + username + "\" placeholder=\"User ID\"></Input>");
		out.println("							<Input id=\"pasw\" name=\"j_password\" value=\"" + password + "\" placeholder=\"Password\" type=\"Password\"></Input>");
		out.println("							<Button width=\"12rem\" text=\"Login\" type=\"Emphasized\" press=\"onLoginTap\"></Button>");
		out.println("						</items>");
		out.println("					</VBox>");
		out.println("				 </content>");
		out.println("			 </Page>");
		out.println("        </App>");
		out.println("    </mvc:View> ");
		out.println("</script>");
		
		out.println("  <script>");
		out.println("sap.ui.controller(\"local.controller\", {");
		out.println("  onLoginTap:function() {");
		out.println("    document.forms[0].submit();");
		out.println("  }");
		out.println("});");

		out.println("var oView = sap.ui.xmlview({");
		out.println("  viewContent: jQuery('#view1').html()");
		out.println("});");
		out.println("oView.placeAt('content');");
		out.println("</script>");

		out.println("</head>");
		out.println("<body class='sapUiBody' style=\"height: 100%;\" >");
		out.println("<form action=\"j_security_check\" method=\"post\">");
		out.println("    <div id=\"content\">");
		out.println("    </div>");
		out.println("</form>");
		out.println("</body>");
		out.println("</html>");
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		doGet(req, resp);
	}

}
