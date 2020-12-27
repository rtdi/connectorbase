package io.rtdi.bigdata.connector.connectorframework.servlet;

import java.io.PrintWriter;

import jakarta.servlet.annotation.WebServlet;

@WebServlet("/ui5/ImpactLineage")
public class ImpactLineagePage extends UI5ServletAbstract {

	private static final long serialVersionUID = 122608943464963L;

	public ImpactLineagePage() {
		super("ImpactLineage", "ImpactLineage");
	}
	
	@Override
	protected void printCustomHeader(PrintWriter out) {
		out.println("<script src=\"//www.amcharts.com/lib/4/core.js\"></script>");
		out.println("<script src=\"//www.amcharts.com/lib/4/charts.js\"></script>");
		out.println("<script src=\"//www.amcharts.com/lib/4/plugins/forceDirected.js\"></script>");
		out.println("<script src=\"https://www.amcharts.com/lib/4/themes/animated.js\"></script>");
	}

}



