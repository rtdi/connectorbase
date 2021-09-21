package io.rtdi.bigdata.connector.connectorframework.rest.entity;

import java.util.LinkedList;
import java.util.List;

import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.entity.ErrorEntity;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.ControllerState;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import jakarta.servlet.ServletContext;
import jakarta.ws.rs.core.SecurityContext;

public class GlobalState {
	private List<ErrorEntity> messages;
	private RoleAssignment roles;
	private ControllerState state;
	
	public GlobalState() {
		super();
	}

	public GlobalState(ServletContext servletContext, SecurityContext securityContext) {
		Exception error = WebAppController.getError(servletContext);
		ConnectorController connectorcontroller = WebAppController.getConnector(servletContext);
		if (connectorcontroller == null) {
			messages = new LinkedList<>();
			messages.add(new ErrorEntity(new PipelineRuntimeException("No ConnectorController found")));
			this.state = ControllerState.TEMPORARYERROR;
		} else {
			messages = connectorcontroller.getErrorList();
			this.state = connectorcontroller.getState();
		}
		if (error != null) {
			messages.add(new ErrorEntity(error));
		}
		roles = new RoleAssignment(securityContext);
	}

	public RoleAssignment getRoles() {
		return roles;
	}
	
	public ControllerState getControllerState() {
		return state;
	}

	public List<ErrorEntity> getMessages() {
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
