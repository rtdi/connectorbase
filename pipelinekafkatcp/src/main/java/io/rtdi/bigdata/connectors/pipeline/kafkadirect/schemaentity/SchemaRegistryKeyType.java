package io.rtdi.bigdata.connectors.pipeline.kafkadirect.schemaentity;

public enum SchemaRegistryKeyType {
	CONFIG("CONFIG"),
	SCHEMA("SCHEMA"),
	MODE("MODE"),
	NOOP("NOOP"),
	DELETE_SUBJECT("DELETE_SUBJECT"),
	CLEAR_SUBJECT("CLEAR_SUBJECT");

	public final String keyType;

	private SchemaRegistryKeyType(String keyType) {
		this.keyType = keyType;
	}

	public static SchemaRegistryKeyType forName(String keyType) {
		for (SchemaRegistryKeyType type : values()) {
			if (type.keyType.equals(keyType)) {
				return type;
			}
		}
		throw new IllegalArgumentException("Unknown schema registry key type : " + keyType);
	}
}
