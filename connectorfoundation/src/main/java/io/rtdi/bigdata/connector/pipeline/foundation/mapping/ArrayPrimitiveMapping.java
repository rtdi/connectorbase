package io.rtdi.bigdata.connector.pipeline.foundation.mapping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;

public class ArrayPrimitiveMapping extends Mapping {
	@SuppressWarnings("unused")
	private Schema arrayschema;
	private PrimitiveMapping recordmapping;

	public ArrayPrimitiveMapping(String expressionarray, Schema arrayschema, String expressionfield) throws IOException {
		super(expressionarray);
		this.arrayschema = arrayschema;
		recordmapping = new PrimitiveMapping(expressionfield);
	}

	public List<Object> apply(List<?> items, JexlRecord context) throws IOException {
		if (items.size() != 0) {
			List<Object> list = new ArrayList<>();
			for (Object item : items) {
				if (item instanceof Record) {
					list.add(recordmapping.evaluate(context));
				}
			}
			return list;
		} else {
			return null;
		}
	}

	public List<Object> apply(int n, JexlRecord context) throws IOException {
		List<Object> list = new ArrayList<>();
		for (int i=0; i<n; i++) {
			list.add(recordmapping.evaluate(context));
		}
		return list;
	}
	
}
