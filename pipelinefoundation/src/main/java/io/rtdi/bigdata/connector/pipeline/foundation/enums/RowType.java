package io.rtdi.bigdata.connector.pipeline.foundation.enums;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesRuntimeException;

/**
 * Provide a hint to the consumer what to do with this row. For example if a row was deleted in the source, the consumer needs to know that,
 *
 */
public enum RowType {
	/**
	 * A brand new record was inserted. A record with this primary key was not present before.
	 * If there is no guarantee such record does not exist yet, use UPSERT instead.
	 */
	INSERT ("I"),
	/**
	 * An existing record was updated.
	 */
	UPDATE ("U"),
	/**
	 * An existing record was deleted, the provided records contains the complete latest version with all payload fields.
	 * If only the primary key of the payload is known, use EXTEMRINATE instead.
	 */
	DELETE ("D"),
	/**
	 * In case either a new record should be created or its last version overwritten, use this UPSERT RowType.
	 */
	UPSERT ("A"),
	/**
	 * When the payload of a delete has null values everywhere except for the primary key fields, then the proper code is EXTERMINATE.
	 * A database would execute a "delete from table where pk = ?" and ignore all other fields.
	 */
	EXTERMINATE ("X"),
	/**
	 * Delete a set of rows at once. An example could be to delete all records of a given patient from the diagnosis table.
	 * In that case the diagnosis table would get a record of type truncate with all payload fields including the PK being null, only the patient field has a value.
	 */
	TRUNCATE ("T"),
	/**
	 * A TRUNCATE followed by the new rows. Example could be a case where all data of a patient should be reloaded. 
	 * A TRUNCATE would be sent to all tables to remove the data and all new data is inserted. But to indicate that this was done via a truncate-replace, the
	 * rows are not flagged as INSERT but REPLACE.
	 * Note that an UPSERT would not work in such scenarios as a patient might have had 10 diagnosis rows but now just 9 are sent. With an UPSERT one would be left over. 
	 */
	REPLACE ("R");

	private String identifer;

	RowType(String identifier) {
		this.identifer = identifier;
	}
	
	public String getIdentifer() {
		return identifer;
	}
	
	public static RowType getByIdentifier(String identifier) throws PropertiesRuntimeException {
		if (identifier != null && identifier.length() > 0) {
			return getByIdentifier(identifier.charAt(0));
		} else {
			throw new PropertiesRuntimeException("Change type cannot be null or an empty string", "wrong change type identifier character", identifier);
		}
	}
	
	public static RowType getByIdentifier(char identifier) throws PropertiesRuntimeException {
		switch (identifier) {
		case 'I': return INSERT;
		case 'U': return UPDATE;
		case 'D': return DELETE;
		case 'A': return UPSERT;
		case 'X': return EXTERMINATE;
		case 'T': return TRUNCATE;
		case 'R': return REPLACE;
		default: throw new PropertiesRuntimeException("Unknow change type \"" + identifier + "\"", "wrong change type identifier character", String.valueOf(identifier));
		}
	}
}
