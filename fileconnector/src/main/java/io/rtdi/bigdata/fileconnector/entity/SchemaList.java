package io.rtdi.bigdata.fileconnector.entity;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;

public class SchemaList {
	private List<FileEntry> files;

	public SchemaList() {
		super();
	}

	public SchemaList(ConnectorController connector) throws IOException {
		files = new ArrayList<>();
		File dir = new File(connector.getConnectorDirectory().getAbsolutePath() + File.separatorChar + ".." + File.separatorChar + "schemas");
		if (!dir.exists()) {
			dir.mkdirs();
		} else if (!dir.isDirectory()) {
			throw new ConnectorCallerException("The schema path exists but is a file, not a directory", null, dir.toString());
		}
		File[] candidates = dir.listFiles();
		Arrays.sort(candidates);
		for (File file : candidates) {
			if (!file.isDirectory() && file.getName().endsWith(".avsc")) {
				files.add(new FileEntry(file));
			}
		}
	}

	public List<FileEntry> getFiles() {
		return files;
	}

	public void setFiles(List<FileEntry> files) {
		this.files = files;
	}

	public static class FileEntry {
		private String filename;
		private String schemaname;
		private Long lastmodified;

		public FileEntry() {
			super();
		}

		public FileEntry(File file) {
			super();
			this.filename = file.getName();
			this.schemaname = filename.replaceFirst("\\.avsc$", "");
			this.lastmodified = file.lastModified();
		}

		public String getFilename() {
			return filename;
		}

		public void setFilename(String filename) {
			this.filename = filename;
		}

		public Long getLastmodified() {
			return lastmodified;
		}

		public void setLastmodified(Long lastmodified) {
			this.lastmodified = lastmodified;
		}

		public String getSchemaname() {
			return schemaname;
		}

		public void setSchemaname(String schemaname) {
			this.schemaname = schemaname;
		}
		
	}
}
