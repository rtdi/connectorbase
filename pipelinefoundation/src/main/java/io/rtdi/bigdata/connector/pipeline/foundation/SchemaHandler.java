package io.rtdi.bigdata.connector.pipeline.foundation;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.metadata.subelements.SchemaMetadataDetails;

/**
 * The SchemaHandler is the tenant specific pointer to a Schema.
 * It consists of a schema name and its metadata.
 *
 */
public class SchemaHandler {
	
	private SchemaName schemaname;
	private SchemaMetadataDetails metadata;
	private IRecordMapping mapping;
	
	/**
	 * Constructs a new SchemaHandler by specifying the optional tenantid and the tenant's schema name.
	 * 
	 * @param tenantid optional tenant id
	 * @param name schemaname within the tenant
	 * @param keyschema Avro schema of the key
	 * @param valueschema Avro schema of the value
	 * @param keyschemaid schema id of the key
	 * @param valueschemaid schema id of the value
	 * @throws PropertiesException in case of any error
	 */
	public SchemaHandler(String tenantid, String name, Schema keyschema, Schema valueschema, int keyschemaid, int valueschemaid) throws PropertiesException {
		this(new SchemaName(tenantid, name), keyschema, valueschema, keyschemaid, valueschemaid); // includes the test if the name is null
	}

	/**
	 * Creates a new SchemaHandler via the global schemaname in the form of {tenantid}-{schemaname}.
	 * 
	 * @param schemaname SchemaName object representing the schema
	 * @param keyschema Avro schema of the key
	 * @param valueschema Avro schema of the value
	 * @param keyschemaid schema id of the key
	 * @param valueschemaid schema id of the value
	 * @throws PropertiesException in case of any error
	 */
	public SchemaHandler(SchemaName schemaname, Schema keyschema, Schema valueschema, int keyschemaid, int valueschemaid) throws PropertiesException {
		if (schemaname == null) {
			throw new PropertiesException("Schemaname cannot be constructed from an empty string");
		}
		this.schemaname = schemaname;
		metadata = new SchemaMetadataDetails(keyschema, valueschema, keyschemaid, valueschemaid); // validates the schemas are present
	}

	@Override
	public int hashCode() {
		return schemaname.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return schemaname.equals(obj);
	}

	/**
	 * @return The global schema name as being set by the constructors
	 */
	public SchemaName getSchemaName() {
		return schemaname;
	}

		
	/**
	 * @return The SchemaMetadata with all schema definitions as stored in the LDM
	 */
	public SchemaMetadataDetails getDetails() {
		return metadata;
	}
	
	@Override
	public String toString() {
		return schemaname.toString();
	}

	/**
	 * 
	 * @return The value-schema definition as Avro Schema 
	 */
	public Schema getValueSchema() {
		return metadata.getValueSchema();
	}

	/**
	 * 
	 * @return The key-schema definition as Avro Schema 
	 */
	public Schema getKeySchema() {
		return metadata.getKeySchema();
	}

	public void setMapping(IRecordMapping mapping) {
		this.mapping = mapping;
	}
	
	public IRecordMapping getMapping() {
		return mapping;
	}

}
