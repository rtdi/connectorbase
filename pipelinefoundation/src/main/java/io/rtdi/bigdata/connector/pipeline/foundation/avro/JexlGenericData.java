package io.rtdi.bigdata.connector.pipeline.foundation.avro;

import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

public class JexlGenericData extends GenericData {
	private static JexlGenericData INSTANCE = new JexlGenericData();

	public JexlGenericData() {
		super();
	}

	public JexlGenericData(ClassLoader classLoader) {
		super(classLoader);
	}
	
	public static GenericData get() { return INSTANCE; }

	public static class JexlRecord extends Record implements IAvroNested {
		private IAvroNested parent = null;

		public JexlRecord(Record other, boolean deepCopy) {
			super(other, deepCopy);
		}
		
	    public JexlRecord(Schema schema) {
	    	super(schema);
	    }

		public void setParent(IAvroNested parent) {
			this.parent = parent;
		}

		public IAvroNested getParent() {
			return parent;
		}
		
	    @Override
	    public void put(String key, Object value) {
	    	super.put(key, value);
	    	if (value instanceof JexlRecord) {
	    		((JexlRecord) value).setParent(this);
	    	} else if (value instanceof JexlArray) {
	    		((JexlArray<?>) value).setParent(this);
	    	}
	    }

	    @Override
	    public void put(int i, Object v) {
	    	super.put(i, v);
	    	if (v instanceof JexlRecord) {
	    		((JexlRecord) v).setParent(this);
	    	} else if (v instanceof JexlArray) {
	    		((JexlArray<?>) v).setParent(this);
	    	}
	    }

	}

	public static class JexlArray<T> extends Array<T> implements IAvroNested {

		private IAvroNested parent = null;

		public JexlArray(Schema schema, Collection<T> c) {
			super(schema, c);
		}

		public JexlArray(int capacity, Schema schema) {
			super(capacity, schema);
		}
		
		public void setParent(IAvroNested parent) {
			this.parent = parent;
		}

		public IAvroNested getParent() {
			return parent;
		}

		@Override
		public boolean add(T o) {
			if (o instanceof JexlRecord) {
				((JexlRecord) o).setParent(this);
			}
			return super.add(o);
		}

		@Override
		public void add(int location, T o) {
			if (o instanceof JexlRecord) {
				((JexlRecord) o).setParent(this);
			}
			super.add(location, o);
		}

		@Override
		public T set(int i, T o) {
			if (o instanceof JexlRecord) {
				((JexlRecord) o).setParent(this);
			}
			return super.set(i, o);
		}
	}

	@Override
	public Object newRecord(Object old, Schema schema) {
		if (old instanceof JexlRecord) {
			JexlRecord record = (JexlRecord) old;
			if (record.getSchema() == schema)
				return record;
		}
		return new JexlRecord(schema);
	}

}
