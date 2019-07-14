package io.rtdi.bigdata.connector.connectorframework.servlet;

import java.io.IOException;
import java.io.InputStream;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import io.rtdi.bigdata.connector.connectorframework.WebAppController;

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
				String error = WebAppController.getError(getServletContext());

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
				if (error != null) {
					out.println("  <f:content>");
					out.println("    <GenericTag text=\"" + error + "\" status=\"Warning\">");
					out.println("      <ObjectAttribute text=\"" + error + "\" emphasized=\"false\" state=\"Warning\"/>");
					out.println("    </m:GenericTag>");
					out.println("  </f:content>");
				}
				out.println("      <f:navigationActions>");
				out.println("        <l:HorizontalLayout>");
				out.println("          <Button icon=\"sap-icon://travel-request\" press=\"onPressStatusLink\" tooltip=\"Connector Status\" />");
				out.println("          <Button icon=\"sap-icon://group-2\" press=\"onPressTopicsLink\" tooltip=\"List of Topics\" />");
				out.println("          <Button icon=\"sap-icon://address-book\" press=\"onPressSchemasLink\" tooltip=\"List of Schemas\" />");
				out.println("          <Button icon=\"sap-icon://browse-folder\" press=\"onPressBrowseLink\" tooltip=\"Browse the Source\" />");
				out.println("          <Button icon=\"sap-icon://org-chart\" press=\"onPressImpactLineageLink\" tooltip=\"Show Impact/Lineage diagram\" />");
				out.println("          <Button icon=\"sap-icon://it-host\" press=\"onPressLandscapeLink\" tooltip=\"Show Landscape diagram\" />");
				out.println("        </l:HorizontalLayout>");
				out.println("      </f:navigationActions>");
				out.println("      <f:actions>");
				out.println("        <Button id=\"editid\"");
				out.println("            text=\"Edit\"");
				out.println("            type=\"Emphasized\"");
				out.println("            enabled=\"{path: 'state>/edit', formatter: '.disableControl'}\"");
				out.println("            press=\"onPressEdit\" />");
				out.println("      </f:actions>");
				out.println("    </f:DynamicPageTitle>");
				out.println("  </f:title>");
				out.println("  <f:header>");
				out.println("  </f:header>");
				out.println("  <f:content>");
				
				int len;
				while ((len = in.read(buffer)) > -1) {
					out.write(buffer, 0, len);
				}
				
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
