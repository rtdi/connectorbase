package io.rtdi.bigdata.connector.pipeline.foundation.metadata.subelements;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

/**
 * The SchemaMetadata class contains all information about a schema, that is most important the key and value Avro-Schema.
 * It also makes sure that both Avro Schemas have the hidden __SchemaID property set which is the ID returned by the Kafka Schema Registry. This allows
 * to read the SchemaID without continous lookups against the Schema Registry.
 *
 */
public class SchemaMetadataDetails {
	
	private Schema keyschema;
	private Schema valueschema;
	private int keyschemaid;
	private int valueschemaid;

	public SchemaMetadataDetails() {
	}


	/**
	 * Create a new SchemaMetadata object by providing the AvroSchema and its SchemaID taken from the Kafka Schema Registry.
	 * 
	 * @param keyschema
	 * @param valueschema
	 * @param keyschemaid
	 * @param valueschemaid
	 * @throws PipelinePropertiesException 
	 */
	public SchemaMetadataDetails(Schema keyschema, Schema valueschema, int keyschemaid, int valueschemaid) throws PropertiesException {
		if (keyschema == null) {
			throw new PropertiesException("keyschema needs to contain an actual Avro schema");
		}
		if (valueschema == null) {
			throw new PropertiesException("valueschema needs to contain an actual Avro schema");
		}
		this.keyschema = keyschema;
		this.valueschema = valueschema;
		this.keyschemaid = keyschemaid;
		this.valueschemaid = valueschemaid;
	}
	
	/**
	 * @return The AvroSchema of the key
	 */
	public Schema getKeySchema() {
		return keyschema;
	}

	/**
	 * @return The AvroSchema of the value
	 */
	public Schema getValueSchema() {
		return valueschema;
	}
			
	@Override
	public int hashCode() {
		return getValueSchema().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof SchemaMetadataDetails) {
			if (obj == this) {
				return true;
			} else {
				SchemaMetadataDetails o = (SchemaMetadataDetails) obj;
				return getValueSchema().equals(o.getValueSchema()) && keyschema.equals(o.keyschema);
			}
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		StringBuffer b = new StringBuffer();
		b.append("keyschema:");
		b.append(keyschema.toString());
		b.append(",\r\nvalueschema: ");
		b.append(valueschema.toString());
		return b.toString();
	}

	public int getKeySchemaID() {
		return keyschemaid;
	}

	public int getValueSchemaID() {
		return valueschemaid;
	}
}
