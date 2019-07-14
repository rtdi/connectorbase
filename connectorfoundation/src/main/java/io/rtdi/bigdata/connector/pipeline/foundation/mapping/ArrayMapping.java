package io.rtdi.bigdata.connector.pipeline.foundation.mapping;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;

public class ArrayMapping extends Mapping {
	private Schema arrayschema;
	private RecordMapping recordmapping;

	public ArrayMapping(String expression, Schema arrayschema) {
		super(expression);
		this.arrayschema = arrayschema;
	}

	public RecordMapping addRecordMapping() {
		this.recordmapping = new RecordMapping(arrayschema);
		return recordmapping;
	}
	
	public List<Object> apply(List<?> items) {
		if (items.size() != 0) {
			List<Object> list = new ArrayList<>();
			for (Object item : items) {
				if (item instanceof JexlRecord) {
					list.add(recordmapping.apply((JexlRecord) item));
				}
			}
			return list;
		} else {
			return null;
		}
	}

	public List<Object> apply(int n, JexlRecord input) {
		List<Object> list = new ArrayList<>();
		for (int i=0; i<n; i++) {
			list.add(recordmapping.apply(input));
		}
		return list;
	}
	
}
