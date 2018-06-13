package com.nc.es.ops;

import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.nc.util.JsonSupport;

public class LookupDeserializer<T> extends JsonDeserializer<T> {

	public static <V> V deserialize(Class<V> type, InputStream is) throws JsonProcessingException, IOException {
		JsonParser parser;
		parser = JsonSupport.mapper().reader().getFactory().createParser(is);
		return new LookupDeserializer<>(type).deserialize(parser, null);
	}

	Class<T> type;

	public LookupDeserializer(Class<T> type) {
		super();
		this.type = type;
	}

	@Override
	public T deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
		while (true) {
			String field;
			final JsonToken token = p.nextToken();
			if (token == JsonToken.FIELD_NAME) {
				field = p.getCurrentName();
			} else if (token == null || token == JsonToken.END_OBJECT) {
				break;
			} else {
				continue;
			}

			p.nextToken();
			switch (field) {
			case "_source":
				return p.readValueAs(type);
			default:
				break;
			}
		}

		return null;
	}

}