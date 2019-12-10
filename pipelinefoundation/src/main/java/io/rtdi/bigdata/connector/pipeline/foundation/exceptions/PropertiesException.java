package io.rtdi.bigdata.connector.pipeline.foundation.exceptions;

import java.io.IOException;

/**
 * Something wrong with the properties.
 *
 */
public class PropertiesException extends IOException {

	private static final long serialVersionUID = 116063643000518740L;
	private String hint = null;
	private String causingobject = null;
	private Integer errorcode = null;

	public PropertiesException() {
		super();
	}

	public PropertiesException(String message) {
		super(message);
	}

	public PropertiesException(String message, Integer errorcode) {
		super(message);
		this.errorcode = errorcode;
	}

	public PropertiesException(Throwable cause) {
		super(cause);
	}

	public PropertiesException(String message, String hint, Integer errorcode, String causingobject) {
		this(message, errorcode);
		this.hint = hint;
		this.causingobject = causingobject;
	}

	public PropertiesException(String message, Throwable cause, String hint) {
		this(message, cause, hint, null);
	}
	
	public PropertiesException(String message, Throwable cause) {
		this(message, cause, null, null);
	}

	/**
	 * The preferred constructor, where an error message, the causing exception, a hint of what the user can do and the object causing the problem.
	 * 
	 * @param message as free form text
	 * @param cause is the causing exception, optional
	 * @param hint provides optional guidance to the user
	 * @param causingobject identifies the input causing the exception
	 */
	public PropertiesException(String message, Throwable cause, String hint, String causingobject) {
		super(message, cause);
		this.hint = hint;
		this.causingobject = causingobject;
	}

	public String getHint() {
		return hint;
	}

	public String getCausingObject() {
		return causingobject;
	}

	public String getErrorHelp() {
		if (errorcode == null) {
			return null;
		} else {
			return "https://github.com/rtdi/connectorbase/blob/master/docs/errors/" + String.valueOf(errorcode) + ".md";
		}
	}

	public String getSourceCodeLink() {
		StackTraceElement line = getStackTrace()[0];
		String filename = line.getFileName();
		int lineno = line.getLineNumber();
		String link = null;
		try {
			Class<?> c = Class.forName(line.getClassName());
			String jarlocation = c.getProtectionDomain().getCodeSource().getLocation().getPath();
			String jarfile = jarlocation.substring(jarlocation.lastIndexOf('/')+1);
			String module = jarfile.substring(0, jarfile.indexOf('-'));
			String packagename = c.getCanonicalName().substring(0, c.getCanonicalName().lastIndexOf(filename.substring(0, filename.lastIndexOf(".java")))-1);
			
			link = "https://github.com/rtdi/connectorbase/blob/master/" + module + "/src/main/java/" + packagename.replace('.', '/') + "/" + filename + "#L" + String.valueOf(lineno);
		} catch (Exception e) {
		}
		return link;
	}

	@Override
	public String toString() {
		String ret = super.toString();
		if (causingobject != null) {
			ret += "\r\nCausing Object: " + causingobject;
		}
		return ret;
	}
}
