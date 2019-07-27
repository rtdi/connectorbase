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
					out.println("sap.ui.define([\"sap/ui/core/mvc/Controller\", \"sap/ui/model/json/JSONModel\"],");
					out.print("function(Controller, JSONModel) {");
					out.println("\"use strict\";");
					out.println("var oStateModel = new JSONModel();");
					out.println("var oGlobalModel = new JSONModel();");
					out.print("return Controller.extend(\"com.rtdi.bigdata.connector.ui.controller.");
					out.print(name);
					out.println("\", {");
					out.println("onPressHomeLink : function(oEvent) {");
					out.println("    window.location.href = './Home';");
					out.println("},");
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
					out.println("onInit : function() {");
					out.println("    this.getView().setModel(oStateModel, \"state\");");
					out.println("    oGlobalModel.loadData(\"../rest/state\", null, false);");
					out.println("    this.getView().setModel(oGlobalModel, \"globalstate\");");
					out.println("    oStateModel.setProperty(\"/edit\", oGlobalModel.getProperty(\"/roles/config\"));");
					out.println("    this.getView().byId('dynamicpageid').setShowFooter(this.showFooter());");
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
					out.println("},");
					out.println("onPressCancel : function(oEvent) {");
					out.println("    this.cancel(oEvent);");
					out.println("},");
					out.println("InputFormatter : function(type) {");
					out.println("    if (type == 'PropertyPassword') {");
					out.println("        return 'Password';");
					out.println("    } else {");
					out.println("        return 'Text';");
					out.println("    }");
					out.println("},");
					out.println("onGlobalErrorPopoverPress : function (oEvent) {"); 
					out.println("    this._getMessagePopover().openBy(oEvent.getSource());"); 
					out.println("},");
					
					out.println("_getMessagePopover : function () {"); 
					out.println("    if (!this._oMessagePopover) {"); 
					out.println("        this._oMessagePopover = sap.ui.xmlfragment(this.getView().getId(),\"com.rtdi.bigdata.connector.ui.fragment.xml.globalerror\", this);"); 
					out.println("        this.getView().addDependent(this._oMessagePopover);"); 
					out.println("    }"); 
					out.println("    return this._oMessagePopover;"); 
					out.println("},");

					String line;
					
					// Provide an option for connectors to add more code
					try (
							InputStream in2 = this.getClass().getClassLoader().getResourceAsStream("/ui5/controller/" + name + "_pre.controller");
						) {
						if (in2 != null) {
							int len;
							byte[] buffer = new byte[4096];
							while ((len = in2.read(buffer)) > -1) {
								out.write(buffer, 0, len);
							}
						}
					}

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

