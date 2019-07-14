package io.rtdi.bigdata.connector.properties.atomic;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
		  use = JsonTypeInfo.Id.NAME, 
		  include = JsonTypeInfo.As.PROPERTY, 
		  property = "type")
@JsonSubTypes({ 
	@Type(value = PropertyString.class, name = "PropertyString"), 
	@Type(value = PropertyArrayList.class, name = "PropertyArrayList"),
	@Type(value = PropertyInt.class, name = "PropertyInt"),
	@Type(value = PropertyLong.class, name = "PropertyLong"), 
	@Type(value = PropertyGroup.class, name = "PropertyGroup"), 
	@Type(value = PropertyPassword.class, name = "PropertyPassword"), 
	@Type(value = PropertyBoolean.class, name = "PropertyBoolean") 
})
public interface IProperty {

	String getName();

}
