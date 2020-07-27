package io.rtdi.bigdata.connector.pipeline.foundation;

public class SchemaConstants {

	/**
	 * Used to identify the change that caused this message in the source system, like a database global transaction id
	 */
	public static final String SCHEMA_COLUMN_SOURCE_TRANSACTION = "__source_transaction";
	/**
	 * The location in the source this change can be found in, e.g. in a file the line number
	 */
	public static final String SCHEMA_COLUMN_SOURCE_ROWID = "__source_rowid";
	/**
	 * The time in millis UTC this change was produced using the producer's clock. Usually System.getMillis().
	 */
	public static final String SCHEMA_COLUMN_CHANGE_TIME = "__change_time";
	/**
	 * An indicator of what kind of change this change message caused, an insert, update, delete,...
	 */
	public static final String SCHEMA_COLUMN_CHANGE_TYPE = "__change_type";
	/**
	 * The producer source system name or the producer name.
	 */
	public static final String SCHEMA_COLUMN_SOURCE_SYSTEM = "__source_system";
	/**
	 * The extension placeholder.
	 */
	public static final String SCHEMA_COLUMN_EXTENSION = "__extension";

}
