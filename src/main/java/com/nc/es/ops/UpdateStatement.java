package com.nc.es.ops;

import static com.nc.es.api.IElasticSearchOps.LINE_BREAK;
import static com.nc.es.api.IElasticSearchOps.UTF_8;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nc.es.api.IElasticSearchObject;
import com.nc.es.tuples.Tuple2;
import com.nc.util.JsonSupport;

public abstract class UpdateStatement implements IElasticSearchObject {

	public static class Doc extends UpdateStatement {

		Object doc;

		public Doc(final Object doc) {
			this.doc = doc;
		}
	}

	public static class DocWithOptions extends WithOptions {
		Object doc;

		public DocWithOptions(final Object doc, final Header h) {
			super();
			this.doc = doc;
			this.h = h;
		}

	}

	public static class Header implements Cloneable {
		@JsonProperty("_id")
		String id;

		@JsonProperty("retry_on_conflict")
		public Integer retries;

		@JsonProperty("_source")
		public Boolean fetchUpdated;

		@JsonProperty("doc_as_upsert")
		public Boolean upsert;

		@Override
		public Header clone() {
			try {
				return (Header) super.clone();
			} catch (final CloneNotSupportedException e) {
				throw new InternalError(e);
			}
		}

		public Header cloneWithId(final String id) {
			final Header h = clone();
			h.id = id;

			return h;
		}
	}

	public static class Script extends UpdateStatement {
		public String source;

		public String lang;

		public Map<String, Object> params;

		@Override
		public Object body() {
			return Tuple2.of("script", this);
		}

		public void setParameter(final String key, final Object val) {
			Map<String, Object> p = params;
			if (p == null) {
				params = p = new HashMap<>();
			}
			p.put(key, val);
		}
	}

	public static class ScriptWithOptions extends WithOptions {

		public String source;

		public String lang;

		public Map<String, Object> params;

		public ScriptWithOptions(final Header h) {
			this.h = h;
		}

		@Override
		public Object body() {
			return Tuple2.of("script", this);
		}

		public void setParameter(final String key, final Object val) {
			Map<String, Object> p = params;
			if (p == null) {
				params = p = new HashMap<>();
			}
			p.put(key, val);
		}
	}

	public abstract static class WithOptions extends UpdateStatement {

		@JsonIgnore
		Header h;

		@Override
		public void writeHeader(final String id, final OutputStream out) throws IOException {
			JsonSupport.toJson(Tuple2.of("update", h.cloneWithId(id)), out);
			out.write(LINE_BREAK);
		}
	}

	// @JsonProperty("_id")
	// @JsonIgnore
	// String id;

	public static <V> UpdateStatement doc(final Tuple2<String, V> t) {
		return new Doc(t.v);
	}

	public static <V> UpdateStatement doc(final Tuple2<String, V> t, final Header h) {
		return new DocWithOptions(t.v, h);
	}

	public static <V> Function<Tuple2<String, V>, UpdateStatement> docWithOptions(final Integer retries, final Boolean fetchUpdated, final Boolean upsert) {
		final Header h = new Header();
		h.retries = retries;
		h.fetchUpdated = fetchUpdated;
		h.upsert = upsert;

		return t -> new DocWithOptions(t.v, h);
	}

	public static <V> Function<Tuple2<String, V>, ScriptWithOptions> scriptWithOptions(final Integer retries, final Boolean fetchUpdated, final Boolean upsert) {
		final Header h = new Header();
		h.retries = retries;
		h.fetchUpdated = fetchUpdated;
		h.upsert = upsert;

		return t -> new ScriptWithOptions(h);
	}

	public Object body() {
		return this;
	}

	@Override
	public String toString() {
		return JsonSupport.toJson(this);
	}

	public void writeHeader(final String id, final OutputStream out) throws IOException {
		// JsonSupport.toJson(Tuple2.of("update", Tuple2.of("_id", id)), out);
		// out.write(LINE_BREAK);
		out.write(String.format("{\"update\":{\"_id\": \"%s\"}}\n", id).getBytes(UTF_8));
	}

	public final void writeStatement(final OutputStream out) {
		JsonSupport.toJson(body(), out);
	}

	public final void writeTo(final String id, final OutputStream out) throws IOException {
		writeHeader(id, out);
		writeStatement(out);
		out.write(LINE_BREAK);
	}
}