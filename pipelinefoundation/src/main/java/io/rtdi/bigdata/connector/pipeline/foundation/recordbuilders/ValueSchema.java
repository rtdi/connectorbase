package io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders;

import java.util.List;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.pipeline.foundation.SchemaConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlArray;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroAnyPrimitive;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroByte;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroNVarchar;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroString;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroTimestamp;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroVarchar;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RowType;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RuleResult;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.IOUtils;

/**
 * A class that helps creating an AvroSchema by code for the value record.
 * It is a custom built Avro schema plus extra columns.
 *
 */
public class ValueSchema extends SchemaBuilder {
	
	public static final String AUDIT_TRANSFORMRESULT_QUALITY = "__transformresult_quality";
	public static final String AUDITTRANSFORMRESULTTEXT = "__transformresult_text";
	public static final String AUDITTRANSFORMATIONNAME = "__transformationname";
	public static final String AUDITDETAILS = "__details";
	public static final String TRANSFORMRESULT = "__transformresult";
	public static final String AUDIT = "__audit";
	public static SchemaBuilder extension;
	public static SchemaBuilder audit;
	private static AvroRecordArray audit_details;
	private static Schema auditdetails_array_schema;
	private static Schema auditdetails_records;
	
	static {
		try {
			extension = new SchemaBuilder("__extension", "Extension point to add custom values to each record");
			extension.add("__path", AvroString.getSchema(), "An unique identifier, e.g. \"street\".\"house number component\"", false);
			extension.add("__value", AvroAnyPrimitive.getSchema(), "The value of any primitive datatype of Avro", false);
			extension.build();
			
			audit = new SchemaBuilder(AUDIT, "If data is transformed this information is recorded here");			
			audit.add(TRANSFORMRESULT, AvroVarchar.getSchema(4), "Is the record PASS, FAILED or WARN?", false);
			audit_details = audit.addColumnRecordArray(AUDITDETAILS, "Details of all transformations", "__audit_details", null);
			audit_details.add(AUDITTRANSFORMATIONNAME, AvroNVarchar.getSchema(1024), "A name identifiying the applied transformation", false);
			audit_details.add(TRANSFORMRESULT, AvroVarchar.getSchema(4), "Is the record PASS, FAIL or WARN?", false);
			audit_details.add(AUDITTRANSFORMRESULTTEXT, AvroNVarchar.getSchema(1024), "Transforms can optionally describe what they did", true);
			audit_details.add(AUDIT_TRANSFORMRESULT_QUALITY, AvroByte.getSchema(), "Transforms can optionally return a percent value from 0 (FAIL) to 100 (PASS)", true);
			audit.build();
			auditdetails_array_schema = IOUtils.getBaseSchema(audit_details.schema());
			auditdetails_records = auditdetails_array_schema.getElementType();
		} catch (SchemaException e) {
			e.printStackTrace();
		}
	}
	
	public static void mergeResults(JexlRecord valuerecord, List<JexlRecord> auditdetails) throws PipelineRuntimeException {
		try {
			JexlRecord auditrecord = (JexlRecord) valuerecord.get(AUDIT);
			RuleResult globalresult;
			if (auditrecord == null) {
				auditrecord = new JexlRecord(audit.getSchema());
				globalresult = RuleResult.PASS;
				valuerecord.put(AUDIT, auditrecord);
			} else {
				globalresult = getRuleResult(auditrecord);
			}
			@SuppressWarnings("unchecked")
			JexlArray<JexlRecord> details = (JexlArray<JexlRecord>) auditrecord.get(AUDITDETAILS);
			if (details == null) {
				details = new JexlArray<JexlRecord>(100, auditdetails_array_schema);
				auditrecord.put(AUDITDETAILS, details);
			}
			for ( JexlRecord d : auditdetails) {
				details.add(d);
				RuleResult ruleresult = getRuleResult(d);
				/*
				 * The individual rule result pulls down the global result PASS -> WARN -> FAIL
				 * but never _corrects_ a result.
				 * Examples:
				 * GLOBAL + INDIVIDUAL = NEW GLOBAL
				 * PASS + PASS = PASS
				 * PASS + WARN = WARN
				 * PASS + FAIL = FAIL
				 * WARN + PASS = WARN
				 * WARN + WARN = WARN
				 * WARN + FAIL = FAIL
				 * FAIL + xxxx = FAIL 
				 */
				if (ruleresult == RuleResult.FAIL) {
					globalresult = RuleResult.FAIL;
				} else if (ruleresult == RuleResult.WARN && globalresult == RuleResult.PASS) {
					globalresult = RuleResult.WARN;
				}
			}
			auditrecord.put(TRANSFORMRESULT, globalresult.name());
		} catch (SchemaException e) {
			throw new PipelineRuntimeException("Cannot merge audit data", e, null);
		}
	}
	
	public static JexlRecord createAuditDetails() {
		return new JexlRecord(auditdetails_records);
	}
	
	public static RowType getChangeType(JexlRecord valuerecord) {
		Object changetype = valuerecord.get(SchemaConstants.SCHEMA_COLUMN_CHANGE_TYPE);
		if (changetype != null) {
			try {
				return RowType.getByIdentifier(changetype.toString());
			} catch (PropertiesRuntimeException e) {
				return null;
			}
		} else {
			return null;
		}
	}
	
	public static RuleResult getRuleResult(JexlRecord record) {
		Object g = record.get(TRANSFORMRESULT);
		RuleResult ruleresult;
		if (g == null) {
			ruleresult = RuleResult.PASS;
		} else {
			String s = g.toString();
			ruleresult = RuleResult.valueOf(s);
		}
		return ruleresult;
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
				false, RowType.UPSERT.name()).setInternal().setTechnical();
		add(SchemaConstants.SCHEMA_COLUMN_CHANGE_TIME, 
				AvroTimestamp.getSchema(),
				"Timestamp of the transaction. All rows of the transaction have the same value.", 
				false, 0L).setInternal().setTechnical();
		add(SchemaConstants.SCHEMA_COLUMN_SOURCE_ROWID, 
				AvroVarchar.getSchema(30),
				"Optional unqiue and static pointer to the row, e.g. Oracle rowid", 
				true).setInternal().setTechnical();
		add(SchemaConstants.SCHEMA_COLUMN_SOURCE_TRANSACTION, 
				AvroVarchar.getSchema(30),
				"Optional source transaction information for auditing", 
				true).setInternal().setTechnical();
		add(SchemaConstants.SCHEMA_COLUMN_SOURCE_SYSTEM, 
				AvroVarchar.getSchema(30),
				"Optional source system information for auditing", 
				true).setInternal().setTechnical();
		addColumnArray(SchemaConstants.SCHEMA_COLUMN_EXTENSION, extension.getSchema(), "Add more columns beyond the official logical data model").setInternal();
		addColumnRecord(AUDIT, audit, "If data is transformed this information is recorded here", true).setInternal();
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
		child.addColumnArray(SchemaConstants.SCHEMA_COLUMN_EXTENSION, extension.getSchema(), "Add more columns beyond the official logical data model");
		return child;
	}

}
