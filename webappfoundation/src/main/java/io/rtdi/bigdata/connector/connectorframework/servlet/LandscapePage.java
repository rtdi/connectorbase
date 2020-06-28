package io.rtdi.bigdata.connector.connectorframework.servlet;

import java.io.PrintWriter;

import javax.servlet.annotation.WebServlet;

@WebServlet("/ui5/Landscape")
public class LandscapePage extends UI5ServletAbstract {

	private static final long serialVersionUID = 122608943469963L;

	public LandscapePage() {
		super("Landscape", "Landscape");
	}
	
	@Override
	protected void printCustomHeader(PrintWriter out) {
		out.println("<script src=\"//www.amcharts.com/lib/4/core.js\"></script>");
		out.println("<script src=\"//www.amcharts.com/lib/4/charts.js\"></script>");
		out.println("<script src=\"//www.amcharts.com/lib/4/plugins/forceDirected.js\"></script>");
		out.println("<script src=\"https://www.amcharts.com/lib/4/themes/animated.js\"></script>");
	}

}
