package io.rtdi.bigdata.connector.properties.atomic;

public interface INamed {

	String getName();

	void setName(String name);

	String getDescription();

	void setDescription(String description);

	String getDisplayname();

	void setDisplayname(String displayname);

	String getIcon();

	void setIcon(String icon);

	Boolean getMandatory();

	void setMandatory(Boolean mandatory);

	Boolean getEnabled();

	void setEnabled(Boolean enabled);

	String getErrormessage();

	void setErrormessage(String errormessage);

	String getErroricon();

	void setErroricon(String erroricon);

}