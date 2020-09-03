package io.rtdi.bigdata.connector.pipeline.foundation.entity;

import java.io.File;
import java.io.InputStream;
import java.util.Properties;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class ErrorEntity {

	private long timestamp;
	protected String message;
	private String exception;
	private String stacktrace;
	private String hint;
	private String causingobject;
	private String sourcecodeline;
	private String errorhelp;
	private String threadname;
	private String stacktracerootcause;
	
	public ErrorEntity() {
		super();
		timestamp = System.currentTimeMillis();
	}
	
	public ErrorEntity(Throwable e) {
		this();
		message = e.getMessage();
		stacktrace = ErrorListEntity.getStackTrace(e);
		stacktracerootcause = ErrorListEntity.getStackTraceRootCause(e);
		exception = e.getClass().getSimpleName();
		this.threadname = Thread.currentThread().getName();
		sourcecodeline = getSourceCodeLink(e);
		if (e instanceof PropertiesException) {
			PropertiesException pe = (PropertiesException) e;
			errorhelp = null;
			hint = pe.getHint();
			causingobject = pe.getCausingObject();
		}
		if (message == null) {
			message = exception;
		}
	}

	public String getSourceCodeLink(Throwable e) {
		StackTraceElement line = null;
		for (StackTraceElement element : e.getStackTrace()) {
			String classname = element.getClassName();
			if (classname.startsWith("io.rtdi")) {
				line = element;
				break;
			}
		}
		if (line != null) {
			String filename = line.getFileName();
			int lineno = line.getLineNumber();
			String link = null;
			try {
				Class<?> c = Class.forName(line.getClassName());
				String jarlocation = c.getProtectionDomain().getCodeSource().getLocation().getPath();
				String jarfile = jarlocation.substring(jarlocation.lastIndexOf('/')+1);
				String packagename = c.getCanonicalName().substring(0, c.getCanonicalName().lastIndexOf(filename.substring(0, filename.lastIndexOf(".java")))-1);
				String module;
				if (jarfile == null || jarfile.length() == 0) {
					// Points to the classes folder and not to the jar file, e.g. .....wtpwebapps/rulesservice/WEB-INF/classes/
					File f = new File(jarlocation);
					f = f.getParentFile();
					f = f.getParentFile();
					module = f.getName();
				} else {
					int p = jarfile.indexOf('-');
					if (p != -1) {
						module = jarfile.substring(0, p);
					} else {
						module = jarfile;
					}
				}
				
				InputStream properties = c.getClassLoader().getResourceAsStream("/" + module + ".properties");
				if (properties != null) {
					Properties props = new Properties();
					props.load(properties);
					
					String basedir = props.getProperty("basedir");
					if (basedir != null) {
						link = basedir + 
								"/src/main/java/" + 
								packagename.replace('.', '/') + "/" + 
								filename + "#L" + String.valueOf(lineno);
					}
				}
			} catch (Exception ignore) {
			}
			return link;
		} else {
			return null;
		}
	}

	public long getTimestamp() {
		return timestamp;
	}

	public String getMessage() {
		return message;
	}

	public String getException() {
		return exception;
	}

	public String getStacktrace() {
		return stacktrace;
	}

	public String getHint() {
		return hint;
	}

	public String getCausingobject() {
		return causingobject;
	}

	public String getSourcecodeline() {
		return sourcecodeline;
	}

	public String getErrorhelp() {
		return errorhelp;
	}
	
	public String getThreadname() {
		return threadname;
	}

	public String getStacktracerootcause() {
		return stacktracerootcause;
	}

	@Override
	public String toString() {
		return exception;
	}
}
