package io.rtdi.bigdata.fileconnector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * <li><pre>00 00 FE FF  = UTF-32, big-endian</pre></li>
 * <li><pre>FF FE 00 00  = UTF-32, little-endian</pre></li>
 * <li><pre>FE FF        = UTF-16, big-endian</pre></li>
 * <li><pre>FF FE        = UTF-16, little-endian</pre></li>
 * <li><pre>EF BB BF     = UTF-8</pre></li>
 *
 */
public class BOMInfo {
	public static final BOMInfo UTF32BE = new BOMInfo(ByteBuffer.wrap(new byte[] {0x00, 0x00, (byte) 0xfe, (byte) 0xff}), Charset.forName("UTF-32BE"));
	public static final BOMInfo UTF32LE = new BOMInfo(ByteBuffer.wrap(new byte[] {(byte) 0xff, (byte) 0xfe, 0x00, 0x00}), Charset.forName("UTF-32LE"));
	public static final BOMInfo UTF16BE = new BOMInfo(ByteBuffer.wrap(new byte[] {(byte) 0xfe, (byte) 0xff}), Charset.forName("UTF-16BE"));
	public static final BOMInfo UTF16LE = new BOMInfo(ByteBuffer.wrap(new byte[] {(byte) 0xff, (byte) 0xfe}), Charset.forName("UTF-16LE"));
	public static final BOMInfo UTF8 = new BOMInfo(ByteBuffer.wrap(new byte[] {(byte) 0xefe, (byte) 0xbb, (byte) 0xbf}), Charset.forName("UTF-8"));
	
	ByteBuffer buffer;
	private Charset charset;
	
	public BOMInfo(ByteBuffer buffer, Charset charset) {
		this.buffer = buffer;
		this.charset = charset;
	}
	
	public int getBOMLength() {
		return buffer.capacity();
	}
	
	public boolean hasBOM(ByteBuffer b) {
		int length = Math.min(b.position(), buffer.capacity());
		for (int i=0; i<length; i++) {
			if (b.array()[i] != buffer.array()[i]) {
				return false;
			}
		}
		return true;
	}
	
	public Charset getCharset() {
		return charset;
	}

	/**
 	 * @param file
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static BOMInfo checkBom(File file) throws IOException {
		ByteBuffer b4 = ByteBuffer.allocate(4);
		try (FileInputStream in = new FileInputStream(file); ) {
			int ret;
			while (b4.remaining() > 0 && (ret = in.read(b4.array(), b4.position(), b4.remaining())) != -1) {
				b4.position(b4.position()+ret);
			}
			if (UTF32BE.hasBOM(b4)) {
				return UTF32BE;
			} else if (UTF32LE.hasBOM(b4)) {
				return UTF32LE;
			} else if (UTF16BE.hasBOM(b4)) {
				return UTF16BE;
			} else if (UTF16LE.hasBOM(b4)) {
				return UTF16LE;
			} else if (UTF8.hasBOM(b4)) {
				return UTF8;
			} else {
				return null;
			}
		}
	}

}
