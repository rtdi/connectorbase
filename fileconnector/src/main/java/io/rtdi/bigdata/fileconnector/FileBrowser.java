package io.rtdi.bigdata.fileconnector;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;

import io.rtdi.bigdata.connector.connectorframework.BrowsingService;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.entity.TableEntry;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.TableType;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.fileconnector.entity.EditSchemaData;

public class FileBrowser extends BrowsingService<FileConnectionProperties> {
	private File schemadir;

	public FileBrowser(ConnectionController controller) throws IOException {
		super(controller);
	}

	/**
	 * Multiple things could go wrong:<br/>
	 * <ul><li>The OS user running the servlet has no permissions on the directory</li>
	 * <li>The directory does not exist</li>
	 * <li>The path is a file, not a directory</li>
	 * <li>The directory is not readable</li></ul>
	 * 
	 * @throws IOException
	 */
	@Override
	protected void open() throws IOException {
		String osuser = System.getProperty("user.name");
		schemadir = EditSchemaData.getSchemaDirectory(controller);
		if (!schemadir.exists()) {
			schemadir.mkdirs();
		}
		if (!schemadir.isDirectory()) {
			throw new PropertiesException("The schema directory exist but is not a directory");
		}
		if (!schemadir.canRead()) {
			throw new PropertiesException("The schema directory cannot be read by the OS user \"" + osuser + "\"");
		}
		if (!schemadir.canWrite()) {
			throw new PropertiesException("The schema directory cannot be written by the OS user \"" + osuser + "\"");
		}
	}

	@Override
	public void close() {
	}

	@Override
	public List<TableEntry> getRemoteSchemaNames() throws IOException {
		List<TableEntry> files = new ArrayList<>();
		File[] candidates = schemadir.listFiles();
		Arrays.sort(candidates);
		for (File file : candidates) {
			if (!file.isDirectory() && file.getName().endsWith(".avsc")) {
				String schemaname = file.getName().substring(0, file.getName().length()-5);
				Schema schema = new Parser().parse(file);
				files.add(new TableEntry(schemaname, TableType.VIEW, schema.getDoc()));
			}
		}
		return files;
	}

	@Override
	public Schema getRemoteSchemaOrFail(String remotename) throws IOException {
		File schemafile = EditSchemaData.getSchemaFile(schemadir, remotename);
		Schema schema = new Parser().parse(schemafile);
		return schema;
	}

	public File getRootDirectory() throws PropertiesException {
		String dir = this.getConnectionProperties().getRootDirectory();
		return new File(dir);
	}

}
