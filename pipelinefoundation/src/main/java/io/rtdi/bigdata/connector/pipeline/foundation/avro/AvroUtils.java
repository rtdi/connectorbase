package io.rtdi.bigdata.connector.pipeline.foundation.avro;

import java.io.IOException;
import java.util.List;

import org.apache.avro.SchemaBuilderException;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.text.StringEscapeUtils;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlArray;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.kafka.avro.RowType;
import io.rtdi.bigdata.kafka.avro.RuleResult;
import io.rtdi.bigdata.kafka.avro.SchemaConstants;
import io.rtdi.bigdata.kafka.avro.recordbuilders.ValueSchema;

public class AvroUtils {
	
	public static String convertRecordToJson(GenericRecord record) throws IOException {
		return record.toString();
	}
	
	public static String encodeJson(String text) {
		/*
		 * Backspace is replaced with \b
		 * Form feed is replaced with \f
		 * Newline is replaced with \n
		 * Carriage return is replaced with \r
		 * Tab is replaced with \t
		 * Double quote is replaced with \"
		 * Backslash is replaced with \\
		 */
		return StringEscapeUtils.escapeJson(text);
	}
	
	public static void mergeResults(JexlRecord valuerecord, List<JexlRecord> auditdetails) throws PipelineRuntimeException {
		try {
			JexlRecord auditrecord = (JexlRecord) valuerecord.get(ValueSchema.AUDIT);
			RuleResult globalresult;
			if (auditrecord == null) {
				auditrecord = new JexlRecord(ValueSchema.audit.getSchema());
				globalresult = RuleResult.PASS;
				valuerecord.put(ValueSchema.AUDIT, auditrecord);
			} else {
				globalresult = getRuleResult(auditrecord);
			}
			@SuppressWarnings("unchecked")
			JexlArray<JexlRecord> details = (JexlArray<JexlRecord>) auditrecord.get(ValueSchema.AUDITDETAILS);
			if (details == null) {
				details = new JexlArray<JexlRecord>(100, ValueSchema.auditdetails_array_schema);
				auditrecord.put(ValueSchema.AUDITDETAILS, details);
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
			auditrecord.put(ValueSchema.TRANSFORMRESULT, globalresult.name());
		} catch (SchemaBuilderException e) {
			throw new PipelineRuntimeException("Cannot merge audit data", e, null);
		}
	}
	
	public static JexlRecord createAuditDetails() {
		return new JexlRecord(ValueSchema.auditdetails_array_schema.getElementType());
	}
	
	public static RowType getChangeType(JexlRecord valuerecord) {
		Object changetype = valuerecord.get(SchemaConstants.SCHEMA_COLUMN_CHANGE_TYPE);
		if (changetype != null) {
			return RowType.getByIdentifier(changetype.toString());
		} else {
			return null;
		}
	}
	
	public static RuleResult getRuleResult(JexlRecord record) {
		Object g = record.get(ValueSchema.TRANSFORMRESULT);
		RuleResult ruleresult;
		if (g == null) {
			ruleresult = RuleResult.PASS;
		} else {
			String s = g.toString();
			ruleresult = RuleResult.valueOf(s);
		}
		return ruleresult;
	}

}
