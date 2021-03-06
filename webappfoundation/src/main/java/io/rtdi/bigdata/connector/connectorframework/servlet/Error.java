package io.rtdi.bigdata.connector.connectorframework.servlet;

import java.io.IOException;
import java.io.PrintWriter;

import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;

@WebServlet("/error")
public class Error extends HttpServlet {

	private static final long serialVersionUID = 8379832402940722307L;

	public Error() {
		super();
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String appname = req.getContextPath();
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
		out.println("Not authenticated, login via <a href=\"" + appname + "/login\">Login Form</a></body></html>");
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String appname = req.getContextPath();
		String target = req.getHeader("referer");
		if (target == null) {
			target = appname + "/";
		}
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
		out.println("Username/Password wrong, please try <a href=\"" + target + "\">again</a></body></html>");
	}

}
