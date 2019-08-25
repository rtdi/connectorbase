package io.rtdi.bigdata.connector.connectorframework.servlet;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.pipeline.foundation.IPipelineAPI;

@WebServlet("/index.html")
public class IndexPage extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public IndexPage() {
        super();
    }

    
	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		response.setContentType("text/html");
		if (WebAppController.getPipelineAPI(getServletContext()) == null) {
			PrintWriter out = response.getWriter();
			out.println("<!DOCTYPE html>");
			out.println("<html><head><title>Check PipelineAPI jar</head><body>");
			out.println("No class found for the PipelineAPI. Seems the corresponding jar file is missing?</body></html>");
		} else {
			IPipelineAPI<?, ?, ?, ?> api = WebAppController.getPipelineAPI(request.getServletContext());
			boolean readsuccess = api.getAPIProperties().getPropertyGroup().isValueSet();
			if (!readsuccess) {
				PrintWriter out = response.getWriter();
				out.println("<!DOCTYPE html>");
				out.println("<html><head><meta http-equiv=\"refresh\" content=\"0; URL=ui5/PipelineConnection\"></head><body>");
				out.println("Either no PipelineConnectionProperties found or they were invalid. Redirecting to <a href=\"PipelineConnection\">PipelineConnection page</a></body></html>");
			} else if (WebAppController.getConnectorFactory(getServletContext()) == null) {
				PrintWriter out = response.getWriter();
				out.println("<!DOCTYPE html>");
				out.println("<html><head><title>Check Connector class</head><body>");
				out.println("No class found for the ConnectorFactory. Should not be possible in fact! Is the ConnectorFactory mentioned "
						+ "in the file src/main/java/META-INF/services/io.rtdi.bigdata.connector.connectorframework.IConnectorFactory</body></html>");
			} else if (WebAppController.getConnector(getServletContext()) == null) {
				PrintWriter out = response.getWriter();
				out.println("<!DOCTYPE html>");
				out.println("<html><head><title>Check Connector class</head><body>");
				out.println("Connector was not created. Should not be possible in fact!</body></html>");
			} else {
				response.setContentType("text/html");
				PrintWriter out = response.getWriter();
				out.println("<!DOCTYPE html>");
				out.println("<html><head><meta http-equiv=\"refresh\" content=\"0; URL=ui5/Home\"></head><body>");
				out.println("Redirecting to the <a href=\"Home.html\">Connector home page</a></body></html>");
			}
		}
	}

}
