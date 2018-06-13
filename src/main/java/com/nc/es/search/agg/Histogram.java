package com.nc.es.search.agg;

public class Histogram extends Aggregation {

	String field;

	int interval;

	int min_doc_count = 1;

	@Override
	AggType type() {
		return AggType.histogram;
	}

}
