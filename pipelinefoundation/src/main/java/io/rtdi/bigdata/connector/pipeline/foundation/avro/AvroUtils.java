package io.rtdi.bigdata.connector.pipeline.foundation.avro;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.text.StringEscapeUtils;

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
}
