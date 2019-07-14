package io.rtdi.bigdata.fileconnector.entity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import javax.xml.bind.annotation.XmlElement;

public class LocaleList {
	private static List<LocaleEntry> locales;
	
	static {
		locales = new ArrayList<>();
		for (Locale locale : Locale.getAvailableLocales()) {
			locales.add(new LocaleEntry(locale));
		}
		Collections.sort(locales);
	}
	
	public LocaleList() {
		super();
	}

	@XmlElement
	public List<LocaleEntry> getLocales() {
		return locales;
	}

	public static class LocaleEntry implements Comparable<LocaleEntry> {
		private String code;
		private String name;
		
		public LocaleEntry() {
		}

		public LocaleEntry(Locale locale) {
			code = locale.toString();
			name = locale.getDisplayName();
		}
		
		@XmlElement
		public String getCode() {
			return code;
		}
		
		@XmlElement
		public String getName() {
			return name;
		}

		@Override
		public int compareTo(LocaleEntry entry) {
			return code.compareTo(entry.code);
		}
		
	}

}
