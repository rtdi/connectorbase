package io.rtdi.bigdata.connector.pipeline.foundation.utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * A couple of helper methods for I/O operations and things close to it.
 *
 */
public class IOUtils {
	static Pattern encoderpattern = Pattern.compile("[\\<\\>\\:\\\\/\\|\\?\\*]");
	static Pattern decoderpattern = Pattern.compile("_x[0-9a-f][0-9a-f][0-9a-f][0-9a-f]");

	public static final int UNDEFINED_INT = -1;
	

	public static final byte MAGIC_BYTE = 0x0;
	
	public static final Charset charset = Charset.forName("UTF-8");

	public static final long METADATAREFERSH = 1000*3600*12; // every 12 hours refresh the metadata
	public static final long METADATAREFERSH_CHANGE_CHECK_FREQUENCY = 60000; // wait for one minute after start of the service before the metadata gets refreshed the first time
	
	private ByteBuffer integerbytebuffer = ByteBuffer.allocate(Integer.BYTES);
	private ByteBuffer longbytebuffer = ByteBuffer.allocate(Long.BYTES);

	private String nextstringvalue = null;
	private long nextlongvalue;
	private int nextintvalue;

	
	/**
	 * Encode a string so it can be used for URL parameters.
	 * {@link java.net.URLEncoder#encode(String, String)}
	 * 
	 * @param s input string
	 * @return encoded string
	 */
	public static String urlEncode(String s) {
		try {
			return URLEncoder.encode(s, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			return null;
		}
	}
	
	/**
	 * Read from an InputStream the entire ByteBuffer size
	 * 
	 * @param in the InputStream to read from
	 * @param b is the ByteBuffer to be filled in its entire
	 * @return true in case all could be read
	 * @throws IOException in case of network issues
	 */
	public static boolean readExact(InputStream in, ByteBuffer b) throws IOException {
		int bytesread = 0;
		int read = 0;
		while (bytesread < b.capacity()) {
			read = in.read(b.array(), b.position(), b.remaining());
			if (read == -1) {
				return false;
			}
			bytesread += read;
			b.position(bytesread);
		}
		return true;
	}
	
	/** Read a string value from the input stream.<br>
	 * The string read is stored in {@link #getNextStringValue()}.<br>
	 * The readNext... method group is mostly used for reading values up the end, when values can exist up to n times.
	 * 
	 * As seen from the wire a string is a an integer with the string length plus the UTF-8 encoded value.
	 * 
	 * @param in the InputStream to read from
	 * @return true in case the data was read successfully
	 * @throws IOException in case of network issues
	 */
	public boolean readNextStringValue(InputStream in) throws IOException {
		if (readExact(in, integerbytebuffer)) {
			int size = integerbytebuffer.getInt(0);
			ByteBuffer textbytes = ByteBuffer.allocate(size);
			if (IOUtils.readExact(in, textbytes)) {
				nextstringvalue = new String(textbytes.array(), IOUtils.charset);
			} else {
				throw new IOException("EOF prior to receiving all data");
			}
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * @return the last string read by the {@link #readNextStringValue(InputStream)} method
	 */
	public String getNextStringValue() {
		return nextstringvalue;
	}
	
	/** Read a long value from the input stream.<br>
	 * The value read is stored in {@link #getNextLongValue()}.<br>
	 * The readNext... method group is mostly used for reading values up the end, when values can exist up to n times.
	 * 
	 * @param in the InputStream to read from
	 * @return true in case the data was read successfully
	 * @throws IOException in case of network issues
	 */
	public boolean readNextLongValue(InputStream in) throws IOException {
		if (readExact(in, longbytebuffer)) {
			nextlongvalue = longbytebuffer.getLong(0);
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * @return the last long value read by a {@link #getNextLongValue()}
	 */
	public long getNextLongValue() {
		return nextlongvalue;
	}

	/**Read a int value from the input stream.<br>
	 * The value read is stored in {@link #getNextIntValue()}.<br>
	 * The readNext... method group is mostly used for reading values up the end, when values can exist up to n times.
	 * 
	 * @param in the InputStream to read from
	 * @return true in case the data was read successfully
	 * @throws IOException in case of network issues
	 */
	public boolean readNextIntValue(InputStream in) throws IOException {
		if (readExact(in, integerbytebuffer)) {
			nextintvalue = integerbytebuffer.getInt(0);
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * @return the last int value read by a {@link #getNextIntValue()}
	 */
	public int getNextIntValue() {
		return nextintvalue;
	}

	/** Send a string over the wire.
	 * 
	 * @param out the OutputStream to write into
	 * @param text String to send
	 * @throws IOException in case of network issues
	 */
	public void sendString(OutputStream out, String text) throws IOException {
		if (text == null) {
			sendInt(out, -1);
		} else if (text.length() == 0) {
			sendInt(out, 0);
		} else {			
			byte[] textbytes = text.getBytes(IOUtils.charset);
			sendInt(out, textbytes.length);
			out.write(textbytes);
		}
	}
	
	/** Read a mandatory string from the wire.
	 * 
	 * @param in the InputStream to read from
	 * @return the string read or null in case the string was null
	 * @throws IOException in case of network issues
	 */
	public String readString(InputStream in) throws IOException {
		int size = readInt(in);
		if (size == 0) {
			return "";
		} else if (size == -1) {
			return null;
		} else {
			ByteBuffer textbytes = ByteBuffer.allocate(size);
			if (IOUtils.readExact(in, textbytes)) {
				return new String(textbytes.array(), IOUtils.charset);
			} else {
				throw new IOException("EOF prior to receiving all data");
			}
		}
	}

	/** Send a byte array over the wire.
	 * 
	 * @param out the OutputStream to write into
	 * @param data byte[] to send
	 * @throws IOException in case of network issues
	 */
	public void sendBytes(OutputStream out, byte[] data) throws IOException {
		if (data != null) {
			sendInt(out, data.length);
			out.write(data);
		} else {
			sendInt(out, 0);
		}
	}
	
	/** Read the data from a mandatory byte array from the wire.
	 * @param in the InputStream to read from
	 * @return the byte[] received
	 * @throws IOException in case of network issues
	 */
	public byte[] readBytes(InputStream in) throws IOException {
		int size = readInt(in);
		if (size == 0) {
			return null;
		} else {
			ByteBuffer bytes = ByteBuffer.allocate(size);
			if (IOUtils.readExact(in, bytes)) {
				return bytes.array();
			} else {
				throw new IOException("EOF prior to receiving all data");
			}
		}
	}

	/** Send an integer over the wire.
	 * 
	 * @param out the OutputStream to write into
	 * @param value of type integer to send 
	 * @throws IOException in case of network issues
	 */
	public void sendInt(OutputStream out, int value) throws IOException {
		integerbytebuffer.putInt(0, value);
		out.write(integerbytebuffer.array());
	}

	/** Read a mandatory int value from the wire
	 * @param in the InputStream to read from
	 * @return integer read from the stream
	 * @throws IOException in case of network issues
	 */
	public int readInt(InputStream in) throws IOException {
		if (readExact(in, integerbytebuffer)) {
			return integerbytebuffer.getInt(0);
		} else {
			throw new IOException("EOF prior to receiving all data");
		}
	}

	/** Send a long over the wire.
	 * @param out the OutputStream to write into
	 * @param value of type long to send
	 * @throws IOException in case of network issues
	 */
	public void sendLong(OutputStream out, long value) throws IOException {
		longbytebuffer.putLong(0, value);
		out.write(longbytebuffer.array());
	}

	/** Read a mandatory long value from the wire
	 * @param in the InputStream to read from
	 * @return long value received
	 * @throws IOException in case of network issues
	 */
	public long readLong(InputStream in) throws IOException {
		if (readExact(in, longbytebuffer)) {
			return longbytebuffer.getLong(0);
		} else {
			throw new IOException("EOF prior to receiving all data");
		}
	}

	/**
	 * @return the name of the host this process is running
	 */
	public static String getHostname() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			return "??";
		}
	}

	/**
	 * In case this schema is a union of null and something else, it returns the _something else_
	 * 
	 * @param schema of the input
	 * @return schema without the union of null, in case it is just that. Can return an union still.
	 */
	public static Schema getBaseSchema(Schema schema) {
		if (schema == null) {
			return null;
		} else if (schema.getType() == Type.UNION) {
			List<Schema> types = schema.getTypes();
			if (types.size() == 2 && types.get(0).getType() == Type.NULL) {
				return types.get(1);
			} else {
				return schema;
			}
		} else {
			return schema;
		}

	}

	/**
	 * Encode a string into a-z chars, escaping all other chars.
	 * @param s input string
	 * @return encoded string with escape chars
	 */
	public static String encodeFileName(String s) {
		/*
	 	< (less than)
		> (greater than)
		: (colon)
		" (double quote)
		/ (forward slash)
		\ (backslash)
		| (vertical bar or pipe)
		? (question mark)
		* (asterisk)
		Integer value zero, sometimes referred to as the ASCII NUL character.
		
		Characters whose integer representations are in the range from 1 through 31
	     */
		s = s.replace("_x", "_x005f_x0078");
		Matcher m = encoderpattern.matcher(s);
		StringBuffer buf = new StringBuffer(s.length());
		while (m.find()) {
			String ch = m.group();
			m.appendReplacement(buf, "_x");
			buf.append(String.format("%1$04x",ch.codePointAt(0)));
		}
		m.appendTail(buf);
		return buf.toString();		
	}

	/** Inverse operation to {@link #encodeFileName(String)}
	 * 
	 * @param name encoded name
	 * @return decoded name
	 */
	public static String decodeFileName(String name) {
		Matcher m = decoderpattern.matcher(name);
		StringBuffer buf = new StringBuffer(name.length());
		while (m.find()) {
			m.appendReplacement(buf, "");
			String ch = m.group().substring(2); // _x0065 is to be replaced
			int utf16char = Integer.parseInt(ch, 16);
			buf.append(Character.toChars(utf16char));
		}
		m.appendTail(buf);
		return buf.toString();		
	}

	public static void deleteDirectory(File dir) throws IOException {
		Files.walk(dir.toPath()).sorted(Comparator.reverseOrder()).forEach(t -> {
			try {
				Files.delete(t);
			} catch (IOException e) {
			}
		});
	}
}
