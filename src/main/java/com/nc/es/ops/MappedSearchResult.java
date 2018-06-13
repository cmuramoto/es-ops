package com.nc.es.ops;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.nc.util.JsonSupport;

public final class MappedSearchResult<T> implements ISearchResult {

	public static class Hit<V> implements Map.Entry<String, V> {

		public String id;

		@JsonProperty("_source")
		public V source;

		@Override
		public String getKey() {
			return id;
		}

		@Override
		public V getValue() {
			return source;
		}

		@Override
		public V setValue(V value) {
			V rv = source;
			source = value;
			return rv;
		}
	}

	public static final class Hits<V> {
		int total;

		List<Hit<V>> hits;
	}

	public static <V> MappedSearchResult<V> deserialize(final InputStream is, final Class<V> type) throws JsonProcessingException, IOException {
		final MappedSearchResultDeserializer<V> d = new MappedSearchResultDeserializer<>(type);

		final JsonParser parser = JsonSupport.mapper().reader().getFactory().createParser(is);

		return d.deserialize(parser, null);
	}

	public int took;

	@JsonProperty("_scroll_id")
	public String scrollId;

	@JsonProperty("timed_out")
	boolean timedOut;

	@JsonProperty("_shards")
	ShardStats shards;

	Hits<T> hits;

	public T first() {
		final Hits<T> h = this.hits;

		return h == null || h.hits == null || h.hits.isEmpty() ? null : h.hits.get(0).source;
	}

	public List<Hit<T>> hits() {
		List<Hit<T>> h;
		if (hits == null || (h = hits.hits) == null || h.isEmpty()) {
			return Collections.emptyList();
		}

		return h;
	}

	public List<String> idList() {
		return ids().collect(Collectors.toList());
	}

	public Stream<String> ids() {
		return hits().stream().map(h -> h.id);
	}

	@Override
	public boolean isEmpty() {
		final Hits<T> h = this.hits;
		return h == null || h.hits == null || h.hits.isEmpty();
	}

	@Override
	public boolean isEmptyOrComplete() {
		final Hits<T> h = this.hits;
		return h == null || h.hits == null || h.hits.isEmpty() || h.hits.size() >= total();
	}

	public T last() {
		final Hits<T> h = this.hits;

		return h == null || h.hits == null || h.hits.isEmpty() ? null : h.hits.get(h.hits.size() - 1).source;
	}

	@Override
	public String scrollId() {
		return scrollId;
	}

	public int size() {
		return hits().size();
	}

	@Override
	public String toString() {
		return asPrettyJson();
	}

	public int total() {
		return hits == null ? 0 : hits.total;
	}

	public Stream<T> vals() {
		return hits().stream().map(h -> h.source);
	}

	public List<T> values() {
		return vals().collect(Collectors.toList());
	}
}