package com.nc.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public final class JsonSupport {

	static Module[] mods;

	static ObjectMapper m;

	static String[] externalModules() {
		return new String[]{ "com.fasterxml.jackson.datatype.jsr310.JavaTimeModule.JavaTimeModule" };
	}

	public static <T> T fromJson(final Class<? extends T> type, final byte[] chunk) {
		try {
			return mapper().readValue(chunk, type);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public static <T> T fromJson(final Class<? extends T> type, final InputStream in) {
		try {
			return mapper().readValue(in, type);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public static <T> T fromJson(final Class<? extends T> type, final Object json) {
		return mapper().convertValue(json, type);
	}

	public static <T> T fromJson(final Class<? extends T> type, final Reader in) {
		try {
			return mapper().readValue(in, type);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public static <T> T fromJson(final Class<? extends T> type, final String s) {
		try {
			return mapper().readValue(s, type);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public static ObjectMapper mapper() {
		ObjectMapper rv = m;

		if (rv == null) {
			rv = m = newMapper();
		}

		return rv;
	}

	private static synchronized Module[] modules() {
		Module[] rv = mods;
		if (rv == null) {
			rv = mods = new Module[]{ new JavaTimeModule(),
					/**
					 * Add as Last Module to Override other (De)-Serializers. This is cheaper than
					 * creating Modifiers.
					 */
					new CommonsModule() };
		}
		return rv;
	}

	public static ObjectMapper newMapper() {
		return newMapper(JsonInclude.Include.NON_EMPTY);
		// // Behavior changed since 2.9.5.
		// return newMapper(JsonInclude.Include.NON_NULL);
	}

	public static ObjectMapper newMapper(final JsonInclude.Include include) {
		final ObjectMapper rv = new ObjectMapper();
		rv.setVisibility(rv.getSerializationConfig().getDefaultVisibilityChecker().//
				withFieldVisibility(JsonAutoDetect.Visibility.ANY).withGetterVisibility(JsonAutoDetect.Visibility.NONE).//
				withSetterVisibility(JsonAutoDetect.Visibility.NONE).withCreatorVisibility(JsonAutoDetect.Visibility.NONE).//
				withIsGetterVisibility(JsonAutoDetect.Visibility.NONE));

		rv.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
		rv.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		rv.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
		rv.configure(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS, false);
		rv.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);

		rv.setSerializationInclusion(include);

		rv.registerModules(modules());

		return rv;
	}

	public static byte[] toBytes(final Object val) {
		try {
			return mapper().writeValueAsBytes(val);
		} catch (final JsonProcessingException e) {
			throw new UncheckedIOException(e);
		}
	}

	public static <T> String toJson(final T val) {
		try {
			return mapper().writeValueAsString(val);
		} catch (final JsonProcessingException e) {
			throw new UnsupportedOperationException(e);
		}
	}

	public static <T> void toJson(final T val, final Appendable dst, final boolean pretty) {
		if (dst instanceof Writer) {
			toJson(val, (Writer) dst, pretty);
		} else if (dst instanceof PrintStream) {
			toJson(val, (PrintStream) dst, pretty);
		} else {
			throw new UnsupportedOperationException();
		}
	}

	public static <T> void toJson(final T val, final OutputStream dst) {
		toJson(val, dst, false);
	}

	public static <T> void toJson(final T val, final OutputStream dst, final boolean pretty) {
		try {
			writer(pretty).writeValue(dst, val);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public static <T> void toJson(final T val, final PrintStream dst, final boolean pretty) {
		try {
			writer(pretty).writeValue(dst, val);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	public static <T> void toJson(final T val, final Writer dst, final boolean pretty) {
		try {
			writer(pretty).writeValue(dst, val);
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	@SuppressWarnings("unchecked")
	public static Map<String, Object> toMap(final Object o) {
		return mapper().convertValue(o, HashMap.class);
	}

	public static <T> String toPrettyJson(final T val) {
		try {
			return mapper().writerWithDefaultPrettyPrinter().writeValueAsString(val);
		} catch (final JsonProcessingException e) {
			throw new UnsupportedOperationException(e);
		}
	}

	static ObjectWriter writer(final boolean pretty) {
		return pretty ? mapper().writerWithDefaultPrettyPrinter() : mapper().writer();
	}
}