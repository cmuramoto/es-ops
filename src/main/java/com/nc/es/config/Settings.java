package com.nc.es.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Settings {

	public class Analysis {
		Map<String, Map<String, Object>> analyzer;

		Map<String, Map<String, Object>> filter;

		Map<String, Map<String, Object>> tokenizer;

		public Analysis analyzer(String an, Map<String, Object> params) {
			if (analyzer == null) {
				analyzer = new HashMap<>(2);
			}
			analyzer.put(an, params);
			return this;
		}

		public Analysis filter(String tk, Map<String, Object> params) {
			if (filter == null) {
				filter = new HashMap<>(2);
			}
			filter.put(tk, params);

			return this;
		}

		public Settings ok() {
			return Settings.this;
		}

		public Analysis tokenizer(String tk, Map<String, Object> params) {
			if (tokenizer == null) {
				tokenizer = new HashMap<>(2);
			}
			tokenizer.put(tk, params);

			return this;
		}
	}

	public static enum StoreType {
		simplefs, niofs, mmapfs, default_fs
	}

	public static Settings get() {
		return new Settings();
	}

	@JsonProperty("number_of_shards")
	Integer shards;

	@JsonProperty("number_of_replicas")
	Integer replicas;

	@JsonProperty("refresh_interval")
	Integer refreshInterval;

	@JsonProperty("max_result_window")
	Integer maxResultWindow;

	@JsonProperty("auto_expand_replicas")
	String autoExpand;

	@JsonProperty("index.store.type")
	StoreType storeType;

	Long creation_date;

	Analysis analysis;

	public Analysis analysis() {
		Analysis an = analysis;

		if (an == null) {
			an = analysis = new Analysis();
		}

		return an;
	}

	public Settings autoExpandingReplicas(int low, int high) {

		if (high == Integer.MAX_VALUE) {
			autoExpand = String.format("%d-all", low);
		} else {
			autoExpand = String.format("%d-%d", low, high);
		}

		return this;
	}

	public long creationDate() {
		return creation_date == null ? 0 : creation_date;
	}

	public Settings installNGram(String label, int min, int max, boolean edge, boolean normalize) {
		final HashMap<String, Object> fp = new HashMap<String, Object>(3);
		fp.put("type", edge ? "edge_ngram" : "ngram");
		fp.put("min_gram", min);
		fp.put("max_gram", max);

		final HashMap<String, Object> ap = new HashMap<String, Object>(3);
		ap.put("type", "custom");
		ap.put("tokenizer", "standard");
		final List<String> filters = new ArrayList<>();
		if (normalize) {
			filters.add("lowercase");
		}
		filters.add(label);
		ap.put("filter", filters);

		return analysis().filter(label, fp).analyzer(label, ap).ok();
	}

	public Settings refreshingEvery(int seconds) {
		refreshInterval = seconds;
		return this;
	}

	public Settings withReplicas(int shards) {
		replicas = shards;
		return this;
	}

	public Settings withShards(int shards) {
		this.shards = shards;
		return this;
	}

}
