package io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes;

public enum AvroDatatypeClass {
	TEXTASCII(0),
	TEXTUNICODE(1),
	
	NUMBER(0),
	
	BINARY(0),
	
	COMPLEX(0),
	
	BOOLEAN(0),
	
	DATETIME(0);

	private int level;

	AvroDatatypeClass(int i) {
		this.level = i;
	}

	public int getLevel() {
		return level;
	}
	

}
