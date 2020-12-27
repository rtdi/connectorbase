package io.rtdi.bigdata.connector.connectorframework.servlet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import jakarta.servlet.ServletException;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

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
		long expiry = System.currentTimeMillis() + UI5ServletAbstract.BROWSER_CACHING_IN_SECS*1000;
		response.setDateHeader("Expires", expiry);
		response.setHeader("Cache-Control", "max-age="+ UI5ServletAbstract.BROWSER_CACHING_IN_SECS);
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
					out.print("return Controller.extend(\"com.rtdi.bigdata.connector.ui.controller.");
					out.print(name);
					out.println("\", {");
					out.println("onPressHomeLink : function(oEvent) {");
					out.println("    window.location.href = './Home';");
					out.println("},");
					out.println("onPressLogoutLink : function(oEvent) {");
					out.println("    window.location.href = '../logout';");
					out.println("},");
					out.println("onInit : function() {");
					out.println("    oStateModel.loadData(\"../rest/state\", null, false);");
					out.println("    this.getView().setModel(oStateModel, \"state\");");
					out.println("    var editpermissions = oStateModel.getProperty(\"/roles/config\");");
					out.println("    this.getView().byId('dynamicpageid').setShowFooter(this.showFooter() && editpermissions);");
					out.println("    this.init();");
					out.println("    if (this.initExtension) {");
					out.println("        this.initExtension();");
					out.println("    }");
					out.println("},");
					out.println("readErrors : function() {");
					out.println("    oStateModel.loadData(\"../rest/state\", null, false);");
					out.println("    this.getView().setModel(oStateModel, \"state\");");
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
					out.println("displayError : function(mesg) {");
					out.println("    var messages = oStateModel.getProperty(\"/messages\");"); 
					out.println("    if (messages) {");
					out.println("        messages.push(mesg);");
					out.println("    } else {");
					out.println("        messages = [mesg];");
					out.println("    }");
					out.println("    oStateModel.setProperty(\"/messages\", messages);");
					out.println("    this.getView().setModel(oStateModel, \"state\");");
					out.println("    sap.m.MessageToast.show(mesg.message);");
					out.println("},");
					out.println("displaySuccess : function(mesg) {");
					out.println("    sap.m.MessageToast.show(mesg.text);");
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

