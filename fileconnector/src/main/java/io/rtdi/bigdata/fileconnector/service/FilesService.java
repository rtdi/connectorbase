package io.rtdi.bigdata.fileconnector.service;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import javax.annotation.security.RolesAllowed;
import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.rest.service.JAXBErrorResponseBuilder;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.fileconnector.FileBrowser;

@Path("/")
public class FilesService {
	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;

	public FilesService() {
	}
			
	@GET
	@Path("/files/{connectionname}/{pattern}/{limit}")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed({ServletSecurityConstants.ROLE_VIEW})
    public Response getFiles(@PathParam("connectionname") String connectionname, @PathParam("pattern") String pattern, @PathParam("limit") int limit) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController connection = connector.getConnectionOrFail(connectionname);
			return Response.ok(new FileList(connection, pattern, limit)).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@GET
	@Path("/files/{connectionname}")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed({ServletSecurityConstants.ROLE_VIEW})
    public Response getFiles(@PathParam("connectionname") String connectionname) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController connection = connector.getConnectionOrFail(connectionname);
			return Response.ok(new FileList(connection)).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	public static class FileList {
		private List<FileEntry> files;

		public FileList() {
			super();
		}

		public FileList(ConnectionController connection) throws IOException {
			files = new ArrayList<>();
			files.add(new FileEntry(connection));
		}

		public FileList(ConnectionController connection, String patternstring, int limit) throws IOException {
			Pattern pattern = Pattern.compile(patternstring);
			files = new ArrayList<>();
			addFiles(connection, pattern, files, limit);
			Collections.sort(files);
		}

		private void addFiles(ConnectionController connection, Pattern pattern, List<FileEntry> files, int limit) throws IOException {
			try (FileBrowser browser = new FileBrowser(connection); ) {
				if (browser.getRootDirectory() != null && files.size() < limit) {
					int rootdirlength = browser.getRootDirectory().getAbsolutePath().length()+1;
					addFiles(browser.getRootDirectory(), pattern, files, rootdirlength, limit);
				}
			}
		}

		private void addFiles(File dir, Pattern pattern, List<FileEntry> files, int rootdirlength, int limit) {
			for (File file : dir.listFiles()) {
				if (files.size() < limit) {
					if (file.isDirectory()) {
						addFiles(file, pattern, files, rootdirlength, limit);
					} else {
						String filename = file.getAbsolutePath().substring(rootdirlength);
						if (filename.endsWith(".processed")) {
							filename = filename.substring(0, filename.lastIndexOf(".processed"));
						}
						if (pattern.matcher(filename).matches()) {
							files.add(new FileEntry(file, rootdirlength));
						}
					}
				} else {
					break;
				}
			}
		}

		public List<FileEntry> getFiles() {
			return files;
		}

		public void setFiles(List<FileEntry> files) {
			this.files = files;
		}

		public static class FileEntry implements Comparable<FileEntry> {
			private String filename;
			private String filetype;
			private Long filesize;
			private Long lastmodified;
			private String path;
			private List<FileEntry> files;

			public FileEntry() {
				super();
			}

			public FileEntry(File file, int rootdirlength) {
				super();
				this.filename = file.getName();
				this.path = file.getAbsolutePath().substring(rootdirlength);
				if (file.isDirectory()) {
					this.filetype = "directory";
					files = new ArrayList<>();
					for (File f : file.listFiles()) {
						files.add(new FileEntry(f, rootdirlength));
					}
					Collections.sort(files);			
				} else {
					this.filetype = "file";
					this.filesize = file.length();
					this.lastmodified = file.lastModified();
				}
			}

			public FileEntry(ConnectionController connection) throws IOException {
				super();
				this.filename = connection.getName();
				this.filetype = "directory";
				files = new ArrayList<>();
				try (FileBrowser browser = new FileBrowser(connection); ) {
					if (browser.getRootDirectory() != null) {
						files = new ArrayList<>();
						int rootdirlength = browser.getRootDirectory().getAbsolutePath().length();
						for (File file : browser.getRootDirectory().listFiles()) {
							files.add(new FileEntry(file, rootdirlength));
						}
						Collections.sort(files);
					}
				}
			}

			public String getFilename() {
				return filename;
			}

			public void setFilename(String filename) {
				this.filename = filename;
			}

			public String getFiletype() {
				return filetype;
			}

			public void setFiletype(String filetype) {
				this.filetype = filetype;
			}

			public Long getFilesize() {
				return filesize;
			}

			public void setFilesize(Long filesize) {
				this.filesize = filesize;
			}

			public Long getLastmodified() {
				return lastmodified;
			}

			public void setLastmodified(Long lastmodified) {
				this.lastmodified = lastmodified;
			}
			
			public List<FileEntry> getFiles() {
				return files;
			}

			public void setFiles(List<FileEntry> files) {
				this.files = files;
			}

			@Override
			public int compareTo(FileEntry o) {
				if (o == null) {
					return -1;
				} else if (o.filetype.equals(filetype)) {
					return filename.compareTo(o.filename);
				} else {
					return filetype.compareTo(o.filetype);
				}
			}

			public String getPath() {
				return path;
			}

			public void setPath(String path) {
				this.path = path;
			}

		
		}
	}
}