package io.rtdi.bigdata.connector.connectorframework.servlet;

import java.io.IOException;
import java.io.InputStream;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet("/ui5/view/*")
public class UI5View extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public UI5View() {
        super();
    }

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		long expiry = System.currentTimeMillis() + UI5ServletAbstract.BROWSER_CACHING_IN_SECS*1000;
		response.setDateHeader("Expires", expiry);
		response.setHeader("Cache-Control", "max-age="+ UI5ServletAbstract.BROWSER_CACHING_IN_SECS);
		byte[] buffer = new byte[4096];
		String resource = request.getPathInfo();
		if (resource.indexOf('/', 1) != -1) {
			throw new ServletException("The requested resource contains relative path information");
		}
		String name = resource.substring(resource.indexOf('/')+1).replace(".view.xml", "");
		
		response.setContentType("application/xml");

		try (
				InputStream in = this.getClass().getClassLoader().getResourceAsStream("/ui5/view/" + name + ".view");
				ServletOutputStream out = response.getOutputStream();
			) {
			if (in != null) {
				out.println("<mvc:View height=\"100%\" class=\"sapUiSizeCompact\" ");
				out.print("controllerName=\"com.rtdi.bigdata.connector.ui.controller.");
				out.print(name);
				out.println("\"");
				out.println("    xmlns:mvc=\"sap.ui.core.mvc\"");
				out.println("    xmlns=\"sap.m\"");
				out.println("    xmlns:f=\"sap.f\"");
				out.println("    xmlns:u=\"sap.ui.unified\"");
				out.println("    xmlns:l=\"sap.ui.layout\"");
				out.println("    xmlns:form=\"sap.ui.layout.form\"");
				out.println("    xmlns:t=\"sap.ui.table\"");
				out.println("    xmlns:app=\"http://schemas.sap.com/sapui5/extension/sap.ui.core.CustomData/1\"");
				out.println("    xmlns:dnd=\"sap.ui.core.dnd\"");
				out.println("    xmlns:components=\"com.rtdi.bigdata.connector.ui.components\"");
				out.println("    xmlns:card=\"sap.f.cards\"");
				out.println("    xmlns:core=\"sap.ui.core\" >");
				out.println("<f:DynamicPage id=\"dynamicpageid\" showFooter=\"false\" fitContent=\"true\" >");
				out.println("  <f:title>");
				out.println("    <f:DynamicPageTitle>");
				out.println("      <f:breadcrumbs>");
				out.println("        <Breadcrumbs links=\"{path:'state>/breadcrumbs', templateShareable:false}\">");
				out.println("          <links>");
				out.println("            <Link text=\"{state>text}\"  href=\"{state>link}\" />");
				out.println("          </links>");
				out.println("        </Breadcrumbs>");
				out.println("      </f:breadcrumbs>");
				out.println("      <f:heading>");
				out.println("        <Title text=\"{state>/title}\" level=\"H1\" />");
				out.println("      </f:heading>");
				out.println("      <f:navigationActions>");
				out.println("          <Button icon=\"sap-icon://home\" press=\"onPressHomeLink\" tooltip=\"Home Page\" />");
				out.println("          <Button icon=\"sap-icon://log\" press=\"onPressLogoutLink\" tooltip=\"Logout\" />");
				out.println("      </f:navigationActions>");
				out.println("      <f:actions>");
				out.println("          <components:ErrorMessageButton");
				out.println("              items=\"{path: 'state>/messages', templateShareable: true}\" >");
				out.println("              <components:items>");
				out.println("                  <components:ErrorMessageItem");
				out.println("                  timestamp=\"{state>timestamp}\"");
				out.println("                  exception=\"{state>exception}\"");
				out.println("                  message=\"{state>message}\"");
				out.println("                  stacktrace=\"{state>stacktrace}\"");
				out.println("                  hint=\"{state>hint}\"");
				out.println("                  causingobject=\"{state>causingobject}\"");
				out.println("                  errorhelp=\"{state>errorhelp}\"");
				out.println("                  sourcecodeline=\"{state>sourcecodeline}\"");
				out.println("                  threadname=\"{state>threadname}\" />");
				out.println("              </components:items>");
				out.println("          </components:ErrorMessageButton>"); 
				out.println("      </f:actions>");
				out.println("    </f:DynamicPageTitle>");
				out.println("  </f:title>");
				out.println("  <f:header>");
				out.println("  </f:header>");
				out.println("  <f:content>");
				out.println("      <Panel height=\"100%\" width=\"100%\" >");
				int len;

				// Provide an option for connectors to add more controls before
				try (
						InputStream in2 = this.getClass().getClassLoader().getResourceAsStream("/ui5/view/" + name + "_pre.view");
					) {
					if (in2 != null) {
						while ((len = in2.read(buffer)) > -1) {
							out.write(buffer, 0, len);
						}
					}
				}
				
				while ((len = in.read(buffer)) > -1) {
					out.write(buffer, 0, len);
				}
				
				// Provide an option for connectors to add more controls after
				try (
						InputStream in2 = this.getClass().getClassLoader().getResourceAsStream("/ui5/view/" + name + "_post.view");
					) {
					if (in2 != null) {
						while ((len = in2.read(buffer)) > -1) {
							out.write(buffer, 0, len);
						}
					}
				}
				out.println("      </Panel>");
				out.println("  </f:content>");
				out.println("  <f:footer>");
				out.println("    <OverflowToolbar>");
				out.println("      <Button type=\"Accept\" id=\"saveid\" text=\"Save\" press=\"onPressSave\" />");
				out.println("      <Button type=\"Reject\" id=\"cancelid\" text=\"Cancel\" press=\"onPressCancel\"  />");
				out.println("    </OverflowToolbar>");
				out.println("  </f:footer>");
				out.println("</f:DynamicPage>");
				out.println("</mvc:View>");
				out.flush();
			} else {
				response.setStatus(404);
			}
		}
	}

}
