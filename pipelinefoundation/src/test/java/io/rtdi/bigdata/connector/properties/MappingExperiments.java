package io.rtdi.bigdata.connector.properties;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.rtdi.bigdata.connector.pipeline.foundation.SchemaConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlArray;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericDatumReader;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroInt;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroLong;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroString;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroTimestamp;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroVarchar;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.AvroRecordArray;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.ValueSchema;

public class MappingExperiments {
	private static EncoderFactory encoderFactory = EncoderFactory.get();
	private static final DecoderFactory decoderFactory = DecoderFactory.get();

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		try {
			Schema s = getValueSchema();
			JexlRecord r = new JexlRecord(s);
			
			r.put(SchemaConstants.SCHEMA_COLUMN_CHANGE_TYPE, "A");
			r.put(SchemaConstants.SCHEMA_COLUMN_CHANGE_TIME, System.currentTimeMillis());
			
			r.put("HOST", "localhost");
			r.put("TIMESTAMP", System.currentTimeMillis());
			r.put("CPU_USER", 2141L);
			r.put("CPU_SYSTEM", 9675L);
			r.put("CPU_IDLE", 76585L);
			r.put("CPU_WAIT_IO", 8335L);
			
			Schema sc = s.getField("CPU_CORES").schema().getTypes().get(1).getElementType();
			JexlRecord rc1 = new JexlRecord(sc);
			rc1.put("CPU_ID", 0);
			rc1.put("CPU_USER", 3463L);
			rc1.put("CPU_SYSTEM", 9347L);
			rc1.put("CPU_IDLE", 29493L);
			rc1.put("CPU_WAIT_IO", 1937L);

			JexlRecord rc2 = new JexlRecord(sc);
			rc2.put("CPU_ID", 1);
			rc2.put("CPU_USER", 9343L);
			rc2.put("CPU_SYSTEM", 8534L);
			rc2.put("CPU_IDLE", 29385L);
			rc2.put("CPU_WAIT_IO", 93274L);
			
			JexlArray<JexlRecord> l = new JexlArray<JexlRecord>(2, s.getField("CPU_CORES").schema().getTypes().get(1));
			l.add(rc1);
			l.add(rc2);
			
			r.put("CPU_CORES", l);
			System.out.println(r.toString());
			
			byte[] bytes;
			try ( ByteArrayOutputStream out = new ByteArrayOutputStream(); ) {
				BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
				DatumWriter<Record> writer = new GenericDatumWriter<>(s);
				writer.write(r, encoder);
				bytes = out.toByteArray();
			}
			
			JexlGenericDatumReader<JexlRecord> reader = new JexlGenericDatumReader<>(s);
			JexlRecord result;
			try (ByteArrayInputStream in = new ByteArrayInputStream(bytes); ) {
				BinaryDecoder decoder = decoderFactory.directBinaryDecoder(in, null);
				result = reader.read(null, decoder);
			}

			System.out.println(result.toString());

			/* RecordMapping m = new RecordMapping(s);
			m.addPrimitiveMapping("HOST", "HOST");
			ArrayMapping a = m.addArrayMapping("CPU_CORES", "CPU_CORES");
			RecordMapping ma = a.addRecordMapping();
			ma.addPrimitiveMapping("CPU_ID", "CPU_ID");
			
			ArrayPrimitiveMapping a1 = m.addArrayPrimitiveMapping("TEXT", "CPU_CORES", "'Text1'");
			
			
			Record out = m.apply(r);
			
			System.out.println(out.toString()); */

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private Schema getValueSchema() throws SchemaException {
		ValueSchema valuebuilder = new ValueSchema("HWMonitor", "HWMonitor structure contains detailed data about the hardware utilization");
		valuebuilder.add("HOST", AvroString.getSchema(), "The ip address of the computer being monitored", false).isPrimaryKey();
		valuebuilder.add("TIMESTAMP", AvroTimestamp.getSchema(), "The timestamp (Unix epoch format) of this event", false).isPrimaryKey();
		valuebuilder.add("DURATION", AvroInt.getSchema(), "The duration for all relative values like CPU_USER", true);
		valuebuilder.add("CPU_USER", AvroLong.getSchema(), "Overall CPU time spent in user mode within the given duration [see Linux /proc/stat]", true);
		valuebuilder.add("CPU_SYSTEM", AvroLong.getSchema(), "Overall CPU time spent in system mode within the given duration [see Linux /proc/stat]", true);
		valuebuilder.add("CPU_IDLE", AvroLong.getSchema(), "Overall CPU time spent idle within the given duration [see Linux /proc/stat]", true);
		valuebuilder.add("CPU_WAIT_IO", AvroLong.getSchema(), "Overall CPU time spent waiting for I/O to complete within the given duration [see Linux /proc/stat]", true);
		valuebuilder.add("PROCESSES_COUNT", AvroInt.getSchema(), "Overall number of processes [see Linux /proc/stat]", true);
		valuebuilder.add("PROCESSES_RUNNING", AvroInt.getSchema(), "Number of running processes [see Linux /proc/stat]", true);
		valuebuilder.add("PROCESSES_BLOCKED", AvroInt.getSchema(), "Number of processes waiting for I/O to complete [see Linux /proc/stat]", true);
		AvroRecordArray cpucorefield = valuebuilder.addColumnRecordArray("CPU_CORES", "Data about the individual cores", true, "CPU_CORES", "Data per CPU core [see Linux /proc/stat]");
		cpucorefield.add("CPU_ID", AvroInt.getSchema(), "The individual CPU id [see Linux /proc/stat]", true);
		cpucorefield.add("CPU_USER", AvroLong.getSchema(), "Overall CPU time spent in user mode within the given duration [see Linux /proc/stat]", true);
		cpucorefield.add("CPU_SYSTEM", AvroLong.getSchema(), "Overall CPU time spent in system mode within the given duration [see Linux /proc/stat]", true);
		cpucorefield.add("CPU_IDLE", AvroLong.getSchema(), "Overall CPU time spent idle within the given duration [see Linux /proc/stat]", true);
		cpucorefield.add("CPU_WAIT_IO", AvroLong.getSchema(), "Overall CPU time spent waiting for I/O to complete within the given duration [see Linux /proc/stat]", true);
		valuebuilder.addColumnArray("TEXT", AvroVarchar.getSchema(40), null, true);
		valuebuilder.build();
		return valuebuilder.getSchema();
	}

}
