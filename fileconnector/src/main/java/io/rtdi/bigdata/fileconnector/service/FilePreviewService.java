package io.rtdi.bigdata.fileconnector.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.security.RolesAllowed;
import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.univocity.parsers.common.processor.ObjectRowListProcessor;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.connectorframework.rest.service.JAXBErrorResponseBuilder;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.fileconnector.FileConnectionProperties;
import io.rtdi.bigdata.fileconnector.entity.EditSchemaData;

@Path("/")
public class FilePreviewService {
	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;

	public FilePreviewService() {
	}
			
	@POST
	@Path("/files/raw/{connectionname}/{filename}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed({ServletSecurityConstants.ROLE_VIEW})
    public Response getRawFileContent(@PathParam("connectionname") String connectionname, @PathParam("filename") String filename, EditSchemaData data) {
		try {
			File file = getFile(servletContext, connectionname, filename);
			return Response.ok(new Filedata(file, getCharset(data))).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@POST
	@Path("/files/parsed/{connectionname}/{filename}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed({ServletSecurityConstants.ROLE_VIEW})
    public Response getParsedFileContent(@PathParam("connectionname") String connectionname, @PathParam("filename") String filename, EditSchemaData data) {
		try {
			File file = getFile(servletContext, connectionname, filename);
			return Response.ok(new ParsedData(file, data)).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@POST
	@Path("/files/guess/{connectionname}/{filename}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed({ServletSecurityConstants.ROLE_VIEW})
    public Response getGuessFormat(@PathParam("connectionname") String connectionname, @PathParam("filename") String filename, EditSchemaData format) {
		try {
			File file = getFile(servletContext, connectionname, filename);
			try (FileInputStream in = new FileInputStream(file); ) {
				CsvParserSettings settings = format.getSettings();
				format.setColumns(null);
				settings.detectFormatAutomatically();
				settings.setNumberOfRecordsToRead(30);
				CsvParser parser = new CsvParser(settings);
				parser.parseAll(in);
				CsvFormat detectedFormat = parser.getDetectedFormat();
				settings.setFormat(detectedFormat);
				
				String[] cols = parser.getRecordMetadata().headers();
				format.updateHeader(cols);
			}
			return Response.ok(format).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	public static File getFile(ServletContext servletContext, String connectionname, String filename) throws PropertiesException {
		ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
		ConnectionController connection = connector.getConnectionOrFail(connectionname);
		FileConnectionProperties props = (FileConnectionProperties) connection.getConnectionProperties();
		String path = props.getRootDirectory() + File.separatorChar + filename;
		File file = new File(path);
		if (!file.exists()) {
			throw new ConnectorCallerException("file not found");
		} else if (!file.isFile()) {
			throw new ConnectorCallerException("path exists but is no file");
		} else {
			return file;
		}
	}
	
	public static Charset getCharset(EditSchemaData data) {
		if (data != null && data.getCharset() != null) {
			return Charset.forName(data.getCharset());
		} else {
			return Charset.defaultCharset();
		}
	}
	
	public static class ParsedData {
		private List<TableColumn> columns;
		private List<Map<String, String>> rows;

		public ParsedData() {
		}

		public ParsedData(File file, EditSchemaData format) throws IOException {
			try (FileInputStream in = new FileInputStream(file); ) {
				CsvParserSettings settings = format.getSettings();
				settings.setNumberOfRecordsToRead(30);
				
				ObjectRowListProcessor rowprocessor = new ObjectRowListProcessor();
				settings.setProcessor(rowprocessor);
									
				CsvParser parser = new CsvParser(settings);
				parser.parse(in, getCharset(format));
				List<Object[]> result = rowprocessor.getRows();
				String[] cols = parser.getRecordMetadata().headers();
				columns = new ArrayList<>();
				if (cols != null) {
					for (String col : cols) {
						columns.add(new TableColumn(col));
					}
				} else {
					// Necessary?
				}
				if (result != null) {
					rows = new ArrayList<>();
					for (Object[] row : result) {
						Map<String, String> value = new HashMap<>();
						for (int i=0; i<row.length && i<columns.size(); i++) {
							if (row[i] != null) {
								value.put(columns.get(i).columnid, row[i].toString());
							}
						}

						rows.add(value);
					}
				}
			}
		}

		public List<TableColumn> getColumns() {
			return columns;
		}

		public List<Map<String, String>> getRows() {
			return rows;
		}

	}
	
	public static class TableColumn {
		private String columnid;
		private String columnname;
		
		public TableColumn() {
		}
		
		public TableColumn(String col) {
			this.columnid = col;
			this.columnname = col;
		}

		public String getColumnId() {
			return columnid;
		}

		public String getColumnName() {
			return columnname;
		}

	}
	
	public static class Filedata {
		private String data;
		/**
		 * The ByteOrderMark is a leading char in the file for Unicode indicating the exact encoding 
		 */
		private String foundbom;

		public Filedata() {
		}
		
		public Filedata(File file, Charset charset) throws IOException {
			try (FileInputStream in = new FileInputStream(file); ) {
				// When no format has been defined yet, dump out the data using the specified codepage
				int ret;
				CharBuffer charbuffer = CharBuffer.allocate(1024*30);
				try (InputStreamReader reader = new InputStreamReader(in, charset); ) {
					while (charbuffer.remaining() > 0 && (ret = reader.read(charbuffer.array(), charbuffer.position(), charbuffer.remaining())) != -1) {
						charbuffer.position(charbuffer.position()+ret);
					}
					data = new String(charbuffer.array(), 0, charbuffer.position()).replace("\r", "\\r").replace("\n", "\\n\n"); // make \r and \n visible
				}
			}
		}

		public String getFoundbom() {
			return foundbom;
		}
		
		public String getSampledata() {
			return data;
		}
	}
	
}
	