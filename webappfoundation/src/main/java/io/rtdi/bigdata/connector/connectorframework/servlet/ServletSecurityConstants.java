package io.rtdi.bigdata.connector.connectorframework.servlet;

public class ServletSecurityConstants {
	/**
	 * Users with the VIEW role can see the status, schemas and topics but no detailed settings like connection properties, no source information, limited data preview
	 */
	public static final String ROLE_VIEW = "connectorview"; 
	/**
	 * Can browse the source, create new schemas, register schemas
	 */
	public static final String ROLE_SCHEMA = "connectorschema"; 
	/**
	 * OPERATOR can restart connectors
	 */
	public static final String ROLE_OPERATOR = "connectoroperator"; 
	/**
	 * User with the CONFIG role can create producers, consumers, connections and modify them
	 */
	public static final String ROLE_CONFIG = "connectorconfig"; 

}
