package io.sap.bigdata.connectors.pipeline.kafkadirect.schemaentity;

import javax.validation.constraints.Min;

import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class SchemaRegistryKey implements Comparable<SchemaRegistryKey> {

	@Min(0)
	protected int magicByte;
	protected SchemaRegistryKeyType keyType;

	public SchemaRegistryKey(@JsonProperty("keytype") SchemaRegistryKeyType keyType) {
		this.keyType = keyType;
	}

	@JsonProperty("magic")
	public int getMagicByte() {
		return this.magicByte;
	}

	@JsonProperty("magic")
	public void setMagicByte(int magicByte) {
		this.magicByte = magicByte;
	}

	@JsonProperty("keytype")
	public SchemaRegistryKeyType getKeyType() {
		return this.keyType;
	}

	@JsonProperty("keytype")
	public void setKeyType(SchemaRegistryKeyType keyType) {
		this.keyType = keyType;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		SchemaRegistryKey that = (SchemaRegistryKey) o;

		if (this.magicByte != that.magicByte) {
			return false;
		}
		if (!this.keyType.equals(that.keyType)) {
			return false;
		}
		return true;
	}

	@Override
	public int hashCode() {
		int result = 31 * this.magicByte;
		result = 31 * result + this.keyType.hashCode();
		return result;
	}

	@Override
	public int compareTo(SchemaRegistryKey otherKey) {
		return this.keyType.compareTo(otherKey.keyType);
	}
}