package io.rtdi.bigdata.connector.connectorframework.rest.entity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.ServletContext;
import jakarta.ws.rs.core.SecurityContext;

import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.JAXBErrorMessage;

public class GlobalState {
	private List<JAXBErrorMessage> messages;
	private RoleAssignment roles;
	
	public GlobalState() {
		super();
	}

	public GlobalState(ServletContext servletContext, SecurityContext securityContext) {
		Exception error = WebAppController.getError(servletContext);
		if (error != null) {
			messages = Collections.singletonList(new JAXBErrorMessage(error));
		} else {
			messages = new ArrayList<>(); // should be an empty array
		}
		roles = new RoleAssignment(securityContext);
	}

	public RoleAssignment getRoles() {
		return roles;
	}

	public List<JAXBErrorMessage> getMessages() {
		return messages;
	}

	public static class RoleAssignment {
		private boolean view;
		private boolean schema;
		private boolean operator;
		private boolean config;

		public RoleAssignment(SecurityContext securityContext) {
			view = securityContext.isUserInRole(ServletSecurityConstants.ROLE_VIEW);
			schema = securityContext.isUserInRole(ServletSecurityConstants.ROLE_SCHEMA);
			operator = securityContext.isUserInRole(ServletSecurityConstants.ROLE_OPERATOR);
			config = securityContext.isUserInRole(ServletSecurityConstants.ROLE_CONFIG);
		}

		public boolean isView() {
			return view;
		}

		public boolean isSchema() {
			return schema;
		}

		public boolean isOperator() {
			return operator;
		}

		public boolean isConfig() {
			return config;
		}

	}
}
