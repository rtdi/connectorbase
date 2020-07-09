package io.rtdi.bigdata.connector.pipeline.foundation.avro;

import java.util.Collection;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.commons.jexl3.JexlContext;

import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroType;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.IOUtils;

public class JexlGenericData extends GenericData {
	private static JexlGenericData INSTANCE = new JexlGenericData();

	public JexlGenericData() {
		super();
	}

	public JexlGenericData(ClassLoader classLoader) {
		super(classLoader);
	}
	
	public static GenericData get() { return INSTANCE; }

	public static class JexlRecord extends Record implements IAvroNested, JexlContext {
		private IAvroNested parent = null;
		private int schemaid;
		private String schemanamerequested;

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
	        Field field = getSchema().getField(key);
	        if (field == null) {
	            throw new AvroRuntimeException("Not a valid schema field: " + key);
	        }
	    	if (value instanceof JexlRecord) {
	    		((JexlRecord) value).setParent(this);
	    	} else if (value instanceof JexlArray) {
	    		((JexlArray<?>) value).setParent(this);
	    	} else {
	    		value = AvroType.getAvroDataType(field.schema()).convertToInternal(value);
	    	}
	    	super.put(field.pos(), value);
	    }

	    @Override
	    public void put(int i, Object v) {
	    	if (v instanceof JexlRecord) {
	    		((JexlRecord) v).setParent(this);
	    	} else if (v instanceof JexlArray) {
	    		((JexlArray<?>) v).setParent(this);
	    	} else {
		        Field field = getSchema().getFields().get(i);
	    		v = AvroType.getAvroDataType(field.schema()).convertToInternal(v);
	    	}
	    	super.put(i, v);
	    }
	    	    
	    public JexlRecord addChild(String key) {
	    	Object f = super.get(key);
	    	if (f == null) {
	    		Field field = this.getSchema().getField(key);
	    		if (field != null) {
	    			Schema s = IOUtils.getBaseSchema(field.schema());
	    			if (s.getType() == Type.ARRAY) {
	    				JexlArray<JexlRecord> a = new JexlArray<>(50, s);
	    				put(key, a);
	    				JexlRecord r = new JexlRecord(s.getElementType());
	    				a.add(r);
	    				return r;
	    			} else if (s.getType() == Type.RECORD) {
	    				JexlRecord r = new JexlRecord(s);
	    				put(key, r);
	    				return r;
	    			} else {
	    				throw new AvroRuntimeException("Column \"" + key + "\" is not a sub record or array of records");
	    			}
	    		} else {
    				throw new AvroRuntimeException("Field \"" + key + "\" does not exist in schema");
    			}
	    	} else if (f instanceof JexlArray) {
	    		@SuppressWarnings("unchecked")
				JexlArray<JexlRecord> a = (JexlArray<JexlRecord>) f;
				JexlRecord r = new JexlRecord(a.getSchema().getElementType());
				a.add(r);
				return r;
	    	} else if (f instanceof JexlRecord) {
				throw new AvroRuntimeException("Field \"" + key + "\" is a record and was added already");
	    	} else {
				throw new AvroRuntimeException("Field \"" + key + "\" is not a sub record or array of records");
			}
	    	
	    }

		@Override
		public String toString() {
			StringBuffer b = new StringBuffer();
			AvroRecord.create().toString(b, this);
			return b.toString();
		}

		@Override
		public void set(String name, Object value) {
			this.put(name, value);
		}

		@Override
		public boolean has(String name) {
			return getSchema().getField(name) != null;
		}

		public int getSchemaId() {
			return schemaid;
		}

		public void setSchemaId(int schemaid) {
			this.schemaid = schemaid;
		}

		/**
		 * The Avro Schema registry might return a schema with a different name than the subject.
		 * This happens if multiple schemas have the same logical structure and just the name is different.
		 * @return the requested schema name
		 */
		public String getSchemaNameRequested() {
			return schemanamerequested;
		}

		public void setSchemaNameRequested(String schemaname) {
			this.schemanamerequested = schemaname;
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
