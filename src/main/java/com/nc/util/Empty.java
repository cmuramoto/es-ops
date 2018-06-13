package com.nc.util;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Recent versions of Jackson (2.9.5+) will consider an instance of {@link java.util.Map} to be
 * empty not only if it has no keys, but also if all of it's values are either null or empty,
 * according to the associated Serializer (take a look at
 * {@link com.fasterxml.jackson.databind.ser.std.MapSerializer#isEmpty(SerializerProvider, java.util.Map)}).
 * <br>
 * This means that Collections.singletonMap("key",new Object()) will be considered empty and will
 * not be serialized. Since ElasticSearch uses empty-constructs such as
 * <b>{"query":{"match_all":{}}}</b>, we must work-around this by supplying an alternative
 * empty-but-serialized representation.
 *
 * @author cmuramoto
 */
@JsonSerialize(using = Empty.class)
public final class Empty extends JsonSerializer<Empty> {

	@Override
	public boolean isEmpty(SerializerProvider provider, Empty value) {
		return false;
	}

	@Override
	public void serialize(Empty value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
		gen.writeStartObject();
		gen.writeEndObject();
	}

}