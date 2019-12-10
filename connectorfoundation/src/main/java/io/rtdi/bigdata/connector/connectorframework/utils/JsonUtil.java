package io.rtdi.bigdata.connector.connectorframework.utils;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

public class JsonUtil {

	private JsonParser parser;

	public JsonUtil(JsonParser parser) throws IOException {
		this.parser = parser;
		parser.nextToken();
	}

	public void getField(String string) throws IOException {
		if (!parser.isClosed()) {
            JsonToken jsonToken = parser.getCurrentToken();
            if (JsonToken.FIELD_NAME.equals(jsonToken)) {
                String fieldname = parser.getCurrentName();
                if (fieldname != null && fieldname.equals(string)) {
                	jsonToken = parser.nextToken();
                } else {
                	throw new JsonParseException(parser, "Expected the json field \"" + string + "\" but got \"" + fieldname + "\"", parser.getTokenLocation());
                }
            } else {
            	throw new JsonParseException(parser, "Expected the json field \"" + string + "\" but got a json token " + jsonToken.name(), parser.getTokenLocation());
            }
		} else {
			throw new JsonParseException(parser, "Expected the json field \"" + string + "\" but json ended already", parser.getTokenLocation());
		}
	}

	public void getArray() throws IOException {
		if (!parser.isClosed()) {
            JsonToken jsonToken = parser.getCurrentToken();
            if (JsonToken.START_ARRAY.equals(jsonToken)) {
            	jsonToken = parser.nextToken();
            } else {
            	throw new JsonParseException(parser, "Expected the an array but got a json token " + jsonToken.name(), parser.getTokenLocation());
            }
		} else {
			throw new JsonParseException(parser, "Expected an array start but json ended already", parser.getTokenLocation());
		}
	}

	public boolean isArrayEnd() throws IOException {
		if (!parser.isClosed()) {
			JsonToken jsonToken = parser.getCurrentToken();
			if (JsonToken.END_ARRAY.equals(jsonToken)) {
				jsonToken = parser.nextToken();
				return true;
			}
			return false;
		} else {
			throw new JsonParseException(parser, "Expected an array start but json ended already", parser.getTokenLocation());
		}
	}

	public void getObject() throws IOException {
		if (!parser.isClosed()) {
			JsonToken jsonToken = parser.getCurrentToken();
			if (JsonToken.START_OBJECT.equals(jsonToken)) {
				jsonToken = parser.nextToken();
			} else {
				throw new JsonParseException(parser, "Expected an object but got a json token " + jsonToken.name(), parser.getTokenLocation());
			}
		} else {
			throw new JsonParseException(parser, "Expected an object start but json ended already", parser.getTokenLocation());
		}
	}

	public String getField() throws IOException {
		if (!parser.isClosed()) {
            JsonToken jsonToken = parser.getCurrentToken();
            if (JsonToken.FIELD_NAME.equals(jsonToken)) {
	            String fieldname = parser.getCurrentName();
	            parser.nextToken();
	            return fieldname;
            } else {
            	throw new JsonParseException(parser, "Expected a json field but got a json token " + jsonToken.name(), parser.getTokenLocation());
            }
		} else {
			throw new JsonParseException(parser, "Expected a json field but json ended already", parser.getTokenLocation());
		}
	}

	public JsonLocation getTokenLocation() {
		return parser.getCurrentLocation();
	}

	public boolean isObjectEnd() throws IOException {
		if (!parser.isClosed()) {
			JsonToken jsonToken = parser.getCurrentToken();
			if (JsonToken.END_OBJECT.equals(jsonToken)) {
				jsonToken = parser.nextToken();
				return true;
			} else {
				return false;
			}
		} else {
			throw new JsonParseException(parser, "Expected a object end start but json ended already", parser.getTokenLocation());
		}
	}

	public void getObjectEnd() throws IOException {
		if (!parser.isClosed()) {
			JsonToken jsonToken = parser.getCurrentToken();
			if (JsonToken.END_OBJECT.equals(jsonToken)) {
				jsonToken = parser.nextToken();
			} else {
				throw new JsonParseException(parser, "Expected a json object end but got a json token " + jsonToken.name(), parser.getTokenLocation());
			}
		} else {
			throw new JsonParseException(parser, "Expected a object end start but json ended already", parser.getTokenLocation());
		}
	}

	public String getText() throws IOException {
		if (!parser.isClosed()) {
			JsonToken jsonToken = parser.getCurrentToken();
			if (JsonToken.VALUE_STRING.equals(jsonToken)) {
				String text = parser.getText();
				jsonToken = parser.nextToken();
				return text;
			} else {
				throw new JsonParseException(parser, "Expected a json string value but got a json token " + jsonToken.name(), parser.getTokenLocation());
			}
		} else {
			throw new JsonParseException(parser, "Expected a object end start but json ended already", parser.getTokenLocation());
		}
	}

}
