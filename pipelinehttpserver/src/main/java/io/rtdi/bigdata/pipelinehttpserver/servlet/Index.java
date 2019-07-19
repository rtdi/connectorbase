package io.rtdi.bigdata.pipelinehttpserver.servlet;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import io.rtdi.bigdata.pipelinehttpserver.ServerStatistics;
import io.rtdi.bigdata.pipelinehttpserver.WebAppController;

@WebServlet("/index.html")
public class Index extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private static ServerStatistics serverstats = new ServerStatistics();
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public Index() {
        super();
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		PrintWriter out = response.getWriter();
		out.append("<HTML><BODY>");
		String error = WebAppController.getError(request.getServletContext());
		if (error != null) {
			out.append("Error occured at start: ");
			out.append(error);
		} else {
			out.append("PipelineHTTPServer is running fine, no startup errors<br>\r\n");
			out.append("<table><tr><th>Measure</th><th>Count</th></tr>\r\n");
			
			out.append("<tr><td>Consumer rows</td><td>");
			out.append(String.valueOf(serverstats.consumerrows));
			out.append("</td></tr>\r\n");

			out.append("<tr><td>Consumer commits</td><td>");
			out.append(String.valueOf(serverstats.consumercommit));
			out.append("</td></tr>\r\n");

			out.append("<tr><td>Producer rows</td><td>");
			out.append(String.valueOf(serverstats.producerrows));
			out.append("</td></tr>\r\n");

			out.append("<tr><td>Producer commits</td><td>");
			out.append(String.valueOf(serverstats.producercommit));
			out.append("</td></tr>\r\n");

			out.append("<tr><td>Schema create</td><td>");
			out.append(String.valueOf(serverstats.registerschema));
			out.append("</td></tr>\r\n");

			out.append("<tr><td>Schema get</td><td>");
			out.append(String.valueOf(serverstats.getschema));
			out.append("</td></tr>\r\n");

			out.append("<tr><td>Topic create</td><td>");
			out.append(String.valueOf(serverstats.newtopic));
			out.append("</td></tr>\r\n");

			out.append("<tr><td>Topic get</td><td>");
			out.append(String.valueOf(serverstats.gettopic));
			out.append("</td></tr>\r\n");
			
			out.append("</table>\r\n");
			
		}
		out.append("</BODY></HTML>");
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}

	public static ServerStatistics getServerStatisticsHandler() {
		return serverstats;
	}
}
