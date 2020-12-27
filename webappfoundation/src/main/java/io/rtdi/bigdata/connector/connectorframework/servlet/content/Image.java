package io.rtdi.bigdata.connector.connectorframework.servlet.content;

import java.io.IOException;
import java.io.InputStream;

import jakarta.servlet.ServletException;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import io.rtdi.bigdata.connector.connectorframework.servlet.UI5ServletAbstract;

@WebServlet("/images/*")
public class Image extends HttpServlet {

	private static final long serialVersionUID = -7479082985611266733L;

	public Image() {
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		byte[] buffer = new byte[4096];
		String resource = request.getPathInfo();
		if (resource.indexOf('/', 1) != -1) {
			throw new ServletException("The requested resource contains relative path information");
		}
		
		response.setContentType("image/png");
		long expiry = System.currentTimeMillis() + UI5ServletAbstract.BROWSER_CACHING_IN_SECS*1000;
		response.setDateHeader("Expires", expiry);
		response.setHeader("Cache-Control", "max-age="+ UI5ServletAbstract.BROWSER_CACHING_IN_SECS);

		try (
				InputStream in = this.getClass().getClassLoader().getResourceAsStream("/images" + resource);
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
