package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineCallerException;

public class AvroRecord implements IAvroDatatype {
	public static final String NAME = "RECORD";
	static AvroRecord element = new AvroRecord();

	private AvroRecord() {
	}

	public static AvroRecord create() {
		return element;
	}
	
	@Override
	public void toString(StringBuffer b, Object value) {
		if (value instanceof Record) {
			Record r = (Record) value;
			Schema schema = r.getSchema();
			b.append('{');
			boolean first = true;
			for (Field f : schema.getFields()) {
				Object v = r.get(f.pos());
				if (v != null) {
					IAvroDatatype datatype = AvroType.getAvroDataType(f.schema());
					if (datatype != null) {
						if (!first) {
							b.append(',');
						} else {
							first = false;
						}
						b.append('\"');
						b.append(f.name());
						b.append("\":");
						datatype.toString(b, v);
					}
				}
			}
			b.append('}');
		}
	}

	@Override
	public GenericRecord convertToInternal(Object value) throws PipelineCallerException {
		if (value == null) {
			return null;
		} else if (value instanceof GenericRecord) {
			return (GenericRecord) value;
		}
		throw new PipelineCallerException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a GenericRecord");
	}

	@Override
	public GenericRecord convertToJava(Object value) throws PipelineCallerException {
		if (value == null) {
			return null;
		} else if (value instanceof GenericRecord) {
			return (GenericRecord) value;
		}
		throw new PipelineCallerException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a GenericRecord");
	}

	@Override
	public Type getBackingType() {
		return Type.RECORD;
	}

	@Override
	public Schema getDatatypeSchema() {
		return null;
	}

	@Override
	public AvroType getAvroType() {
		return AvroType.AVRORECORD;
	}

	@Override
	public String toString() {
		return NAME;
	}

}
