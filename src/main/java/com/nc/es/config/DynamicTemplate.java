package com.nc.es.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.nc.es.config.Property.GeneralType;
import com.nc.es.config.Property.Type;

public class DynamicTemplate {

	public static DynamicTemplate keywordDefaults() {
		final DynamicTemplate dt = of(GeneralType.STRING);
		dt.mapping = Property.get().with(Type.KEYWORD);
		return dt;
	}

	public static DynamicTemplate of(GeneralType type) {
		final DynamicTemplate dt = new DynamicTemplate();
		dt.type = type;
		return dt;
	}

	@JsonProperty("match_mapping_type")
	GeneralType type;

	String match;

	String unmatch;

	String path_match;

	String path_unmatch;

	String match_pattern;

	Property mapping;

	public DynamicTemplate mappingTo(Property mapping) {
		this.mapping = mapping;
		return this;
	}

	public DynamicTemplate matching(String match) {
		this.match = match;
		return this;
	}

	public DynamicTemplate pathMatching(String path_match) {
		this.path_match = path_match;
		return this;
	}

	public DynamicTemplate pathUnMatching(String path_unmatch) {
		this.path_unmatch = path_unmatch;
		return this;
	}

	public DynamicTemplate unMatching(String match) {
		unmatch = match;
		return this;
	}

	public DynamicTemplate useJavaRegex() {
		match_pattern = "regex";

		return this;
	}

}
