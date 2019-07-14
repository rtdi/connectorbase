package io.sap.bigdata.connectors.pipeline.kafkadirect.schemaentity;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;

public class Converter {
	private static ObjectMapper objectmapper = new ObjectMapper();
	private static TypeReference<Map<String, Object>> maptype = new TypeReference<Map<String, Object>>() {};

	public Converter() {
	}
	
	public static SchemaRegistryKey getKey(byte[] keybytes) throws PipelineRuntimeException {
		try {
			Map<String, Object> keyvalues = objectmapper.readValue(keybytes, maptype);
			SchemaRegistryKeyType keytype = SchemaRegistryKeyType.forName((String) keyvalues.get("keytype"));
			switch (keytype) {
			case CLEAR_SUBJECT:
			case CONFIG:
			case DELETE_SUBJECT:
			case MODE:
			case NOOP:
				return null;
			case SCHEMA:
				return objectmapper.readValue(keybytes, SchemaKey.class);
			default:
				return null;
			}
		} catch (IOException e) {
			throw new PipelineRuntimeException("The key entry in the schema registry topic is not of an expected format", e, null, new String(keybytes));
		}
	}

	public static SchemaValue getSchema(byte[] valuebytes) throws PipelineRuntimeException {
		try {
			return objectmapper.readValue(valuebytes, SchemaValue.class);
		} catch (IOException e) {
			throw new PipelineRuntimeException("The value entry in the schema registry topic is not of an expected format", e, null, new String(valuebytes));
		}
	}
	
	public static byte[] serilalizeKey(SchemaKey schemakey) throws PipelineRuntimeException {
		try {
			return objectmapper.writeValueAsBytes(schemakey);
		} catch (JsonProcessingException e) {
			throw new PipelineRuntimeException("The schema registry entry failed to be serialized to Json", e, null, schemakey.toString());
		}
	}

	public static byte[] serilalizeValue(SchemaValue schemavalue) throws PipelineRuntimeException {
		try {
			return objectmapper.writeValueAsBytes(schemavalue);
		} catch (JsonProcessingException e) {
			throw new PipelineRuntimeException("The schema registry entry failed to be serialized to Json", e, null, schemavalue.toString());
		}
	}

}
