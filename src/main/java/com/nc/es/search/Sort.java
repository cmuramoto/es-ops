package com.nc.es.search;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.nc.es.api.IElasticSearchObject;
import com.nc.es.tuples.Tuple2;

public class Sort implements IElasticSearchObject {

	public static enum Mode {
		min, max, avg, sum, median;
	}

	public static enum Order {
		asc, desc;
	}

	public static Sort asc() {
		final Sort s = new Sort();
		s.order = Order.asc;
		return s;
	}

	public static Sort desc() {
		final Sort s = new Sort();
		s.order = Order.desc;
		return s;
	}

	Order order;

	Mode mode;

	@JsonProperty("nested_path")
	String nestedPath;

	@JsonProperty("nested_filter")
	Tuple2<String, Tuple2<String, String>> nestedFilter;

	String missing;

	public Sort missingFirst() {
		missing = "_first";
		return this;
	}

	public Sort missingLast() {
		missing = "_last";
		return this;
	}

	public Sort nested(String path) {
		this.nestedPath = path;
		return this;
	}

	public Sort nestedFilter(String path, String term) {
		this.nestedFilter = Tuple2.of("term", Tuple2.of(path, term));
		return this;
	}

	@Override
	public String toString() {
		return asPrettyJson();
	}

	public Sort with(Sort.Mode mode) {
		this.mode = mode;
		return this;
	}
}