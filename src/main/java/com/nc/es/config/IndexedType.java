package com.nc.es.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({ "dynamic", "source", "all", "ttl", "timestamp", "properties" })
public class IndexedType {

	public static IndexedType get() {
		return new IndexedType();
	}

	@JsonProperty("_source")
	Source source;

	@JsonProperty("_all")
	All all;

	// @JsonProperty("_ttl")
	// TTL ttl;

	// @JsonProperty("_timestamp")
	// Timestamp timestamp;

	Boolean dynamic;

	Map<String, Property> properties;

	@JsonProperty("dynamic_templates")
	List<Map<String, DynamicTemplate>> dynamicTemplates;

	@JsonProperty("dynamic_date_formats")
	Set<String> dynDateFormats;

	public IndexedType add(String name, Property prop) {
		Map<String, Property> p = properties;
		if (p == null) {
			p = properties = new HashMap<>();
		}
		p.put(name, prop);

		return this;
	}

	public IndexedType autoDetectingDatesWith(String... patterns) {
		Set<String> formats = this.dynDateFormats;
		if (formats == null) {
			formats = dynDateFormats = new HashSet<>(2);
		}
		for (final String pattern : patterns) {
			formats.add(pattern);
		}
		return this;
	}

	public IndexedType disableAll() {
		all = new All();
		all.enabled = false;
		return this;
	}

	// public IndexedType disableTTL() {
	// this.ttl = null;
	// return this;
	// }

	public IndexedType dynamic() {
		this.dynamic = true;
		return this;
	}

	// public IndexedType enableTTL() {
	// this.ttl = new TTL();
	// return this;
	// }

	public Map<String, Property> properties() {
		return properties;
	}

	public IndexedType with(Source source) {
		this.source = source;
		return this;
	}

	public IndexedType with(String label, DynamicTemplate template) {
		List<Map<String, DynamicTemplate>> templates = this.dynamicTemplates;

		if (templates == null) {
			templates = dynamicTemplates = new ArrayList<>(2);
		}

		templates.add(Collections.singletonMap(label, template));
		return this;
	}

	// public IndexedType withTimestamp(boolean ignoreMissing) {
	// this.timestamp = new Timestamp();
	// timestamp.ignoreMissing = true;
	// timestamp.enabled = true;
	//
	// return this;
	// }
}
