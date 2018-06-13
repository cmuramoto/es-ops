package com.nc.es.ops;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class SimpleSearchResult implements ISearchResult {

	public static class Hit implements Map.Entry<String, Map<String, Object>> {

		@JsonProperty("_id")
		public String id;

		@JsonProperty("_source")
		public Map<String, Object> source;

		public String id() {
			return id;
		}

		@Override
		public String getKey() {
			return id;
		}

		@Override
		public Map<String, Object> getValue() {
			return source;
		}

		@Override
		public Map<String, Object> setValue(Map<String, Object> value) {
			Map<String, Object> rv = source;
			source = value;
			return rv;
		}
	}

	public static final class Hits {
		int total;

		List<Hit> hits;
	}

	public int took;

	@JsonProperty("_scroll_id")
	public String scrollId;

	@JsonProperty("timed_out")
	boolean timedOut;

	@JsonProperty("_shards")
	ShardStats shards;

	Hits hits;

	public Map<String, Object> aggregations;

	public List<Hit> hits() {
		List<Hit> h;
		if (hits == null || (h = hits.hits) == null || h.isEmpty()) {
			return Collections.emptyList();
		}

		return h;
	}

	public List<String> idList() {
		return ids().collect(Collectors.toList());
	}

	public Stream<String> ids() {
		return hits().stream().map(Hit::id);
	}

	@Override
	public boolean isEmpty() {
		final Hits h = hits;
		return h == null || h.hits == null || h.hits.isEmpty();
	}

	@Override
	public boolean isEmptyOrComplete() {
		final Hits h = hits;
		return h == null || h.hits == null || h.hits.isEmpty() || h.hits.size() >= total();
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
}