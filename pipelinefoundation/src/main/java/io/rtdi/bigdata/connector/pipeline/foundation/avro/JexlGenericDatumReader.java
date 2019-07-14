package io.rtdi.bigdata.connector.pipeline.foundation.avro;

import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;

public class JexlGenericDatumReader<D> extends GenericDatumReader<D> {

	public JexlGenericDatumReader() {
		super(null, null, JexlGenericData.get());
	}

	public JexlGenericDatumReader(GenericData data) {
		super(data);
	}

	public JexlGenericDatumReader(Schema writer, Schema reader, GenericData data) {
		super(writer, reader, data);
	}

	public JexlGenericDatumReader(Schema writer, Schema reader) {
		super(writer, reader, JexlGenericData.get());
	}

	public JexlGenericDatumReader(Schema schema) {
		super(schema, schema, JexlGenericData.get());
	}


	@SuppressWarnings("rawtypes")
	@Override
	protected Object newArray(Object old, int size, Schema schema) {
		if (old instanceof Collection) {
			return super.newArray(old, size, schema);
		} else
			return new JexlGenericData.JexlArray(size, schema);
	}

}
