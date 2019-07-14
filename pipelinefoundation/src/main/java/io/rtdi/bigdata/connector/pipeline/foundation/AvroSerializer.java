package io.rtdi.bigdata.connector.pipeline.foundation;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

/**
 * Creates a a binary representation of an Avro GenericRecord in the Kafka format.
 * Hence all data putin into Kafka can be read by other tools like Kafka Connect.  
 *
 */
public class AvroSerializer {
	
	private static EncoderFactory encoderFactory = EncoderFactory.get();


	public static byte[] serialize(int schemaid, GenericRecord data) throws IOException {
		try ( ByteArrayOutputStream out = new ByteArrayOutputStream(); ) {
			out.write(IOUtils.MAGIC_BYTE);
			out.write(ByteBuffer.allocate(Integer.BYTES).putInt(schemaid).array());
			BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
			DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(data.getSchema());
			writer.write(data, encoder);
			encoder.flush();
			byte[] bytes = out.toByteArray();
			return bytes;
		} catch (Exception e) {
			if (e instanceof IOException) {
				throw e;
			} else {
				throw new IOException(e);
			}
		}
	}

}
