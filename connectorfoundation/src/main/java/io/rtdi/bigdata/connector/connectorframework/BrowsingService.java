package io.rtdi.bigdata.connector.connectorframework;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.entity.TableEntry;
import io.rtdi.bigdata.connector.properties.ConnectionProperties;

/**
 *  *
 * @param <S> ConnectionProperties
 */
public abstract class BrowsingService<S extends ConnectionProperties> implements Closeable {

	protected S connectionproperties;
	protected ConnectionController controller;
	protected final Logger logger;

	/**
	 * Opens a connection to the source system by calling open().
	 * 
	 * @param controller ConnectionController
	 * @throws IOException if network errors
	 */
	@SuppressWarnings("unchecked")
	public BrowsingService(ConnectionController controller) throws IOException {
		super();
		this.connectionproperties = (S) controller.getConnectionProperties();
		this.controller = controller;
		this.logger = LogManager.getLogger(this.getClass().getName() + "." + this.toString());
		open();
	}

	public S getConnectionProperties() {
		return connectionproperties;
	}
	
	/**
	 * This method implementation should open the connection to the source system but be very verbose about possible errors.<br>
	 * For example, when opening a JDBC connection, returning just the JDBC error might not be enough for the business user to find
	 * the root cause. It could be
	 * <ul><li>Network down</li>
	 * <li>Hostname not known</li>
	 * <li>host cannot be reached</li>
	 * <li>database down</li>
	 * <li>username/password incorrect</li>
	 * </ul>
	 * Usually called by the constructor but in case of an error, this method might be called again to re-open the connection
	 *   
	 * @throws IOException if network errors
	 */
	public abstract void open() throws IOException;
	
	@Override
	public abstract void close();

	/**
	 * Create a list of all remote objects the system knows.
	 * 
	 * @return a list of all found remote tables
	 * @throws IOException if network error
	 */
	public abstract List<TableEntry> getRemoteSchemaNames() throws IOException;

	/**
	 * Return the table definition as Avro schema.
	 * 
	 * @param remotename of the object to return metadata for
	 * @return Schema of the remote object
	 * @throws IOException if network error
	 */
	public abstract Schema getRemoteSchemaOrFail(String remotename) throws IOException;
	
	/**
	 * Validate that all connection properties are valid and a connection can be established
	 * @throws IOException on errors
	 */
	public abstract void validate() throws IOException;

	/**
	 * Delete the schema with the provided name.
	 * 
	 * @param remotename of the schema
	 * @throws IOException on errors
	 */
	public abstract void deleteRemoteSchemaOrFail(String remotename) throws IOException;
}
