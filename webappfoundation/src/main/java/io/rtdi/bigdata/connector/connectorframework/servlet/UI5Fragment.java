package io.rtdi.bigdata.connector.connectorframework.servlet;

import java.io.IOException;
import java.io.InputStream;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet("/ui5/fragment/xml/*")
public class UI5Fragment extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public UI5Fragment() {
        super();
    }

	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		byte[] buffer = new byte[4096];
		String resource = request.getPathInfo();
		if (resource.indexOf('/', 1) != -1) {
			throw new ServletException("The requested resource contains relative path information");
		}
		String name = resource.substring(resource.indexOf('/')+1).replace(".fragment.xml", "");
		
		response.setContentType("application/xml");

		try (
				InputStream in = this.getClass().getClassLoader().getResourceAsStream("/ui5/fragment/xml/" + name + ".xml");
				ServletOutputStream out = response.getOutputStream();
			) {
			if (in != null) {
				int len;
				while ((len = in.read(buffer)) > -1) {
					out.write(buffer, 0, len);
				}
			} else {
				response.setStatus(404);
			}
		}
	}

}
