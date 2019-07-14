package io.rtdi.bigdata.fileconnector.entity;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

import javax.xml.bind.annotation.XmlElement;

public class CharsetList {

	private static List<CharsetEntry> charsets;
	
	static {
		SortedMap<String, Charset> availablecharsets = Charset.availableCharsets();
		charsets = new ArrayList<>();
		for (Charset charset : availablecharsets.values()) {
			charsets.add(new CharsetEntry(charset));
		}
	}
	
	public CharsetList() {
		super();
	}

	@XmlElement
	public List<CharsetEntry> getCharsets() {
		return charsets;
	}

	public static class CharsetEntry {
		private String code;
		private String name;
		
		public CharsetEntry() {
			
		}
		
		public CharsetEntry(Charset charset) {
			code = charset.name();
			name = charset.displayName();
		}
		
		@XmlElement
		public String getCode() {
			return code;
		}
		
		@XmlElement
		public String getName() {
			return name;
		}
	}

}
