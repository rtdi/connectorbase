package io.rtdi.bigdata.connector.connectorframework.servlet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet("/ui5/controller/*")
public class UI5Controller extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public UI5Controller() {
        super();
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String resource = request.getPathInfo();
		if (resource.indexOf('/', 1) != -1) {
			throw new ServletException("The requested resource contains relative path information");
		}
		String name = resource.substring(resource.indexOf('/')+1).replace(".controller.js", "");
		
		response.setContentType("application/javascript");

		try (
				InputStream in = this.getClass().getClassLoader().getResourceAsStream("/ui5/controller/" + name + ".controller");
				ServletOutputStream out = response.getOutputStream();
				) {
			if (in != null) {
				try (BufferedReader reader = new BufferedReader(new InputStreamReader(in));) {
					out.println("sap.ui.define([");
					out.println(reader.readLine());
					out.println("],");
					out.print("function(");
					out.print(reader.readLine());
					out.println(") {");
					out.println("\"use strict\";");
					out.print("return Controller.extend(\"com.rtdi.bigdata.connector.ui.controller.");
					out.print(name);
					out.println("\", {");
					out.println("onPressStatusLink : function(oEvent) {");
					out.println("    window.location.href = 'ConnectorStatus';");
					out.println("},");
					out.println("onPressTopicsLink : function(oEvent) {");
					out.println("    window.location.href = './Topics';");
					out.println("},");
					out.println("onPressSchemasLink : function(oEvent) {");
					out.println("    window.location.href = './Schemas';");
					out.println("},");
					out.println("onPressImpactLineageLink : function(oEvent) {");
					out.println("    window.location.href = './ImpactLineage';");
					out.println("},");
					out.println("onPressLandscapeLink : function(oEvent) {");
					out.println("    window.location.href = './Landscape';");
					out.println("},");
					out.println("onPressBrowseLink : function(oEvent) {");
					out.println("    window.location.href = './Browse';");
					out.println("},");
					out.println("onPressEdit : function(oEvent) { ");
					out.println("    var page = this.getView().byId(\"dynamicpageid\");");
					out.println("    page.setShowFooter(true);");
					out.println("    var oStateModel = this.getView().getModel(\"state\");");
					out.println("    var ret = oStateModel.setProperty(\"/edit\", true );");
					out.println("    this.edit(oEvent);");
					out.println("},");
					out.println("onInit : function() {");
					out.println("    var oStateModel = new JSONModel();");
					out.println("    oStateModel.setData( { \"edit\" : false } );");
					out.println("    this.getView().setModel(oStateModel, \"state\");");
					out.println("    this.init();");
					out.println("},");
					out.println("enableControl : function(value) {");
					out.println("    return !!value;");
					out.println("},");
					out.println("disableControl : function(value) {");
					out.println("    return !value;");
					out.println("},");
					out.println("onPressSave : function(oEvent) {");
					out.println("    this.save(oEvent)");
					out.println("    var oStateModel = this.getView().getModel(\"state\");");
					out.println("    var ret = oStateModel.setProperty(\"/edit\", false );");
					out.println("    var page = this.getView().byId(\"dynamicpageid\");");
					out.println("    page.setShowFooter(false);");
					out.println("},");
					out.println("onPressCancel : function(oEvent) {");
					out.println("    var oStateModel = this.getView().getModel(\"state\");");
					out.println("    var ret = oStateModel.setProperty(\"/edit\", false );");
					out.println("    var page = this.getView().byId(\"dynamicpageid\");");
					out.println("    page.setShowFooter(false);");
					out.println("    this.cancel(oEvent);");
					out.println("},");
					out.println("InputFormatter : function(type) {");
					out.println("    if (type == 'propertyPassword') {");
					out.println("        return 'Password';");
					out.println("    } else {");
					out.println("        return 'Text';");
					out.println("    }");
					out.println("},");

					String line;
					while ((line = reader.readLine()) != null) {
						out.println(line);
					}
					
					out.println("});");
					out.println("});");
				}
			} else {
				response.setStatus(404);
			}
		}
	}

}

