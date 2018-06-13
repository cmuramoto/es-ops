package com.nc.es.search.agg;

import com.nc.es.tuples.Tuple2;

public class Terms extends Aggregation {

	public static Terms on(String field) {
		final Terms terms = new Terms();
		terms.field = field;

		return terms;
	}

	String field;

	int size = 1;

	Tuple2<String, String> order;

	public Terms asc() {
		order = Tuple2.of("_term", "asc");
		return this;
	}

	public Terms desc() {
		order = Tuple2.of("_term", "desc");
		return this;
	}

	@Override
	AggType type() {
		return AggType.terms;
	}

	public Terms upTo(int size) {
		this.size = size;
		return this;
	}

}