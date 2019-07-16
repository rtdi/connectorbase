package io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders;

import io.rtdi.bigdata.connector.pipeline.foundation.SchemaConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroAnyPrimitive;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroByte;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroString;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroTimestamp;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroVarchar;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;

/**
 * A class that helps creating an AvroSchema by code for the value record.
 * It is a custom built Avro schema plus extra columns.
 *
 */
public class ValueSchema extends SchemaBuilder {
	
	public static SchemaBuilder extension;
	public static SchemaBuilder audit;
	
	static {
		try {
			extension = new SchemaBuilder("__extension", "Extension point to add custom values to each record");
			extension.add("__path", AvroString.getSchema(), "An unique identifier, e.g. \"street\".\"house number component\"", false);
			extension.add("__value", AvroAnyPrimitive.getSchema(), "The value of any primitive datatype of Avro", false);
			extension.build();
			
			audit = new SchemaBuilder("__audit", "If data is transformed this information is recorded here");			
			audit.add("__transformresult", AvroString.getSchema(), "Is the record PASS, FAILED or WARN?", false);
			AvroRecordArray audit_details = audit.addColumnRecordArray("__details", "Details of all transformations", true, "__audit_details", null);
			audit_details.add("__transformationname", AvroString.getSchema(), "A name identifiying the applied transformation", false);
			audit_details.add("__transformresult", AvroString.getSchema(), "Is the record PASS, FAILED or WARN?", false);
			audit_details.add("__transformresult_text", AvroString.getSchema(), "Transforms can optionally describe what they did", true);
			audit_details.add("__transformresult_quality", AvroByte.getSchema(), "Transforms can optionally return a percent value from 0 (FAIL) to 100 (PASS)", true);
			audit.build();
		} catch (SchemaException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * In order to create a complex Avro schema for the value record from scratch, this builder is used.<br>
	 * It adds mandatory columns to the root level and optional extension columns.
	 *  
	 * @param name of the schema
	 * @param namespace optional, to make sure two schemas with the same name but different meanings can be separated
	 * @param description optional text
	 * @throws SchemaException if the schema is invalid
	 */
	public ValueSchema(String name, String namespace, String description) throws SchemaException {
		super(name, namespace, description);
		add(SchemaConstants.SCHEMA_COLUMN_CHANGE_TYPE, 
				AvroVarchar.getSchema(1),
				"Indicates how the row is to be processed: Insert, Update, Delete, upsert/Autocorrect, eXterminate, Truncate", 
				false).setInternal().setTechnical();
		add(SchemaConstants.SCHEMA_COLUMN_CHANGE_TIME, 
				AvroTimestamp.getSchema(),
				"Timestamp of the transaction. All rows of the transaction have the same value.", 
				false).setInternal().setTechnical();
		add(SchemaConstants.SCHEMA_COLUMN_SOURCE_ROWID, 
				AvroVarchar.getSchema(30),
				"Optional unqiue and static pointer to the row, e.g. Oracle rowid", 
				true).setInternal().setPrimaryKey().setTechnical();
		add(SchemaConstants.SCHEMA_COLUMN_SOURCE_TRANSACTION, 
				AvroVarchar.getSchema(30),
				"Optional source transaction information for auditing", 
				true).setInternal().setTechnical().setPrimaryKey();
		add(SchemaConstants.SCHEMA_COLUMN_SOURCE_SYSTEM, 
				AvroVarchar.getSchema(30),
				"Optional source system information for auditing", 
				true).setInternal().setTechnical().setPrimaryKey();
		addColumnArray("__extension", extension.getSchema(), "Add more columns beyond the official logical data model", true).setInternal();
		addColumnRecord("__audit", audit, "If data is transformed this information is recorded here", true).setInternal();
	}

	/**
	 * @param name of the value schema
	 * @param description free for text
	 * @throws SchemaException if the schema is invalid
	 * @see #ValueSchema(String, String, String)
	 */
	public ValueSchema(String name, String description) throws SchemaException {
		this(name, null, description);
	}

	/**
	 * While a normal child schema has just the added columns, a child schema of the ValueSchema has an additional extension column always.
	 * 
	 * @see SchemaBuilder#createNewSchema(String, String)
	 */
	@Override
	protected SchemaBuilder createNewSchema(String name, String schemadescription) throws SchemaException {
		SchemaBuilder child = super.createNewSchema(name, schemadescription);
		child.addColumnArray("__extension", extension.getSchema(), "Add more columns beyond the official logical data model", true);
		return child;
	}

}
