package io.rtdi.bigdata.connector.connectorframework.servlet;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

@WebServlet("/error")
public class Error extends HttpServlet {

	private static final long serialVersionUID = 8379832402940722307L;

	public Error() {
		super();
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		PrintWriter out = resp.getWriter();
		try {
			HttpSession session = req.getSession(false);
			if (session != null) {
				req.getSession(false).invalidate();
			}
		} catch (IllegalStateException e) {
		}
		out.println("<!DOCTYPE html>");
		out.println("<html><head></head><body>");
		out.println("Not authenticated, login via <a href=\"login\">Login Form</a></body></html>");
	}

}
