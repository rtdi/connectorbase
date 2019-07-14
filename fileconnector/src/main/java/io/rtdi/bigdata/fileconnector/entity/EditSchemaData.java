package io.rtdi.bigdata.fileconnector.entity;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import javax.xml.bind.annotation.XmlTransient;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Parser;
import org.apache.avro.Schema.Type;
import org.apache.commons.text.StringEscapeUtils;

import com.univocity.parsers.csv.CsvParserSettings;

import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroNVarchar;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroType;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroVarchar;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.AvroField;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.ValueSchema;

public class EditSchemaData {
	private static final String FILENAMEPATTERN = "FILENAMEPATTERN";
	private static final String LOCALE = "LOCALE";
	private static final String CHARSET = "CHARSET";
	private static final String DELIMITER = "DELIMITER";
	private static final String LINESEPARATOR = "LINESEPARATOR";
	private static final String QUOTE = "QUOTE";
	private static final String QUOTEESCAPE = "QUOTEESCAPE";
	private static final String ISHEADEREXTRACTIONENABLED = "ISHEADEREXTRACTIONENABLED";
	
	private String schemaname;
	private String description;
	private String filenamepattern;
	private String charset;
	private String locale;
	private CsvParserSettings settings = new CsvParserSettings();
	
	private List<ColumnDefinition> columns;
	
	private static String defaultcharset = Charset.defaultCharset().name();
	private static String defaultlocale = Locale.getDefault().toString();
		
	public EditSchemaData() {
		super();
		locale = defaultlocale;
		charset = defaultcharset;
		setDelimiter(";");
		setHeaderExtractionEnabled(true);
		setLineSeparator("\n");
		setQuote("\"");
		setQuoteEscape("\"");
	}

	public EditSchemaData(File schemafile) throws IOException {
		this();
		if (!schemafile.exists()) {
			throw new ConnectorCallerException(
					"The file with the schema definition cannot be found", 
					"In the webapp root directory, the schema subdirectory, all *.avsc file are stored", 
					schemafile.getPath());
		}
		if (!schemafile.canRead()) {
			throw new ConnectorCallerException("The file with the schema definition exists but cannot be read by the OS user", null, schemafile.getPath());
		}
		Schema schema = new Parser().parse(schemafile);
		setSchemaname(schema.getName());
		setDescription(schema.getDoc());
		setCharset(schema.getProp(CHARSET));
		setLocale(schema.getProp(LOCALE));
		setFilenamepattern(schema.getProp(FILENAMEPATTERN));
		setDelimiter(schema.getProp(DELIMITER));
		setLineSeparator(schema.getProp(LINESEPARATOR));
		setQuote(schema.getProp(QUOTE));
		setQuoteEscape(schema.getProp(QUOTEESCAPE));
		setHeaderExtractionEnabled(schema.getProp(ISHEADEREXTRACTIONENABLED).equalsIgnoreCase("true"));
		List<ColumnDefinition> columns = new ArrayList<>();
		for (Field f : schema.getFields()) {
			String name = AvroField.getOriginalName(f);
			if (!AvroField.isInternal(f)) {
				columns.add(new ColumnDefinition(name, AvroType.getAvroDatatype(f.schema()), f.schema().getType() == Type.UNION));
			}
		}
		setColumns(columns);
	}

	public String getSchemaname() {
		return schemaname;
	}

	public void setSchemaname(String schemaname) {
		this.schemaname = schemaname;
	}

	public String getFilenamepattern() {
		return filenamepattern;
	}

	public void setFilenamepattern(String filenamepattern) {
		this.filenamepattern = filenamepattern;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) throws ConnectorCallerException {
		if (charset != null) {
			if (Charset.availableCharsets().containsKey(charset)) {
				this.charset = charset;
			} else {
				throw new ConnectorCallerException("The provided charset is not known to this system", null, charset);
			}
		} else {
			this.charset = null;
		}
	}

	public String getLocale() {
		return locale;
	}

	public void setLocale(String locale) throws ConnectorCallerException {
		if (locale != null) {
			boolean found = false;
			for (Locale e : Locale.getAvailableLocales()) {
				if (e.toString().equals(locale)) {
					found = true;
					break;
				}
			}
			if (found) {
				this.locale = locale;
			} else {
				throw new ConnectorCallerException("The provided locale is not known to this system", null, locale);
			}
		} else {
			this.locale = null;
		}
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getLineSeparator() {
		return StringEscapeUtils.escapeJava(settings.getFormat().getLineSeparatorString());
	}

	public void setLineSeparator(String lineseparator) {
		settings.getFormat().setLineSeparator(StringEscapeUtils.unescapeJava(lineseparator));
	}

	public String getDelimiter() {
		return StringEscapeUtils.escapeJava(String.valueOf(settings.getFormat().getDelimiter()));
	}

	public void setDelimiter(String delimiter) {
		settings.getFormat().setDelimiter(StringEscapeUtils.unescapeJava(delimiter));
	}
	
	public String getQuoteEscape() {
		return StringEscapeUtils.escapeJava(String.valueOf(settings.getFormat().getQuoteEscape()));
	}

	public void setQuoteEscape(String quoteescape) {
		String s = StringEscapeUtils.unescapeJava(quoteescape);
		if (s != null && s.length() > 0) {
			settings.getFormat().setQuoteEscape(s.charAt(0));
		}
	}

	public String getQuote() {
		return StringEscapeUtils.escapeJava(String.valueOf(settings.getFormat().getQuote()));
	}

	public void setQuote(String quote) {
		String s = StringEscapeUtils.unescapeJava(quote);
		if (s != null && s.length() > 0) {
			settings.getFormat().setQuote(s.charAt(0));
		}
	}

	public String getCharToEscapeQuoteEscaping() {
		return StringEscapeUtils.escapeJava(String.valueOf(settings.getFormat().getCharToEscapeQuoteEscaping()));
	}

	public void setCharToEscapeQuoteEscaping(String escape) {
		String s = StringEscapeUtils.unescapeJava(escape);
		if (s != null && s.length() > 0) {
			settings.getFormat().setCharToEscapeQuoteEscaping(s.charAt(0));
		}
	}

	public boolean isHeaderExtractionEnabled() {
		return settings.isHeaderExtractionEnabled();
	}

	public void setHeaderExtractionEnabled(boolean headerextractionenabled) {
		settings.setHeaderExtractionEnabled(headerextractionenabled);
	}
	
	@XmlTransient
	public CsvParserSettings getSettings() {
		return settings;
	}
	
	public List<ColumnDefinition> getColumns() {
		return columns;
	}
	
	public void setColumns(List<ColumnDefinition> columns) {
		this.columns = columns;
		if (columns != null) {
			String[] headers = new String[columns.size()];
			for( int i=0; i<columns.size(); i++) {
				headers[i] = columns.get(i).getName();
			}
			settings.setHeaders(headers);
		} else {
			settings.setHeaders((String[]) null); 
		}
	}
	
	public void updateHeader(String[] headers) {
		settings.setHeaders(headers);
		if (columns == null) {
			columns = new ArrayList<>();
			for (String name : headers) {
				columns.add(new ColumnDefinition(name, AvroNVarchar.create(100).toString(), true));
			}
		} else {
			for (int i=0; i<headers.length; i++) {
				if (i<columns.size()) {
					ColumnDefinition c = columns.get(i);
					c.name = headers[i];
				} else {
					columns.add(new ColumnDefinition(headers[i], AvroNVarchar.create(100).toString(), true));
				}
			}
		}
	}

	public void writeSchema(File schemafile) throws SchemaException, IOException {
		try (FileWriter out = new FileWriter(schemafile);) {
			out.write(createSchema().toString(true));
		}
	}

	public Schema createSchema() throws SchemaException, IOException {
		ValueSchema schema = new ValueSchema(schemaname, description);
		Schema s = schema.getSchema();
		s.addProp(FILENAMEPATTERN, filenamepattern);
		s.addProp(LOCALE, locale);
		s.addProp(CHARSET, charset);
		s.addProp(DELIMITER, getDelimiter());
		s.addProp(LINESEPARATOR, getLineSeparator());
		s.addProp(QUOTE, getQuote());
		s.addProp(QUOTEESCAPE, getQuoteEscape());
		s.addProp(ISHEADEREXTRACTIONENABLED, String.valueOf(isHeaderExtractionEnabled()));
		
		schema.add("FILENAME", 
				AvroVarchar.getSchema(300),
				"Name of the file", 
				true).setPrimaryKey().setInternal();

		if (columns == null) {
			schema.add("col1", AvroNVarchar.getSchema(1000), null, true); // Avro Schemas need at least one field
		} else {
			for (ColumnDefinition column : columns) {
				Schema dt = AvroType.getSchemaFromDataTypeRepresentation(column.datatype);
				if (dt != null) {
					schema.add(column.name, dt, null, column.optional);
				}
			}
		}
		schema.build();
		return schema.getSchema();
	}

	public static File getSchemaFile(File schemadir, String schemaname) {
		String schemafilepath = schemadir.getAbsolutePath() + File.separatorChar + schemaname + ".avsc";
		return new File(schemafilepath);
	}
	
	public static File getSchemaDirectory(File rootdirectory) {
		String schemapath = rootdirectory.getAbsolutePath() + File.separatorChar + "schemas";
		return new File(schemapath);
	}

	public static File getSchemaDirectory(ConnectionController connection) {
		String srootpath = connection.getDirectory().getAbsolutePath();
		File rootdir = new File(srootpath);
		return getSchemaDirectory(rootdir);
	}

	
	public static class ColumnDefinition {

		private String name;
		private String datatype;
		private boolean optional = true;

		public ColumnDefinition() {
		}
		
		public ColumnDefinition(String header, String datatype, boolean optional) {
			this.name = header;
			this.datatype = datatype;
			this.optional = optional;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getDatatype() {
			return datatype;
		}

		public void setDatatype(String datatypestring) {
			this.datatype = datatypestring;
		}

		public boolean isOptional() {
			return optional;
		}

		public void setOptional(boolean optional) {
			this.optional = optional;
		}
		
	}

}
