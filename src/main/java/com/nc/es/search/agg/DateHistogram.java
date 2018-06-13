package com.nc.es.search.agg;

import java.util.concurrent.TimeUnit;

public class DateHistogram extends Aggregation {

	public static DateHistogram on(String field) {
		final DateHistogram dh = new DateHistogram();
		dh.field = field;
		return dh;
	}

	String field;

	String interval;

	int min_doc_count = 1;

	public DateHistogram daily(int value) {
		interval = value + "d";
		return this;
	}

	private String map(TimeUnit tu) {
		switch (tu) {
		case DAYS:
			return "d";
		case HOURS:
			return "h";
		case MINUTES:
			return "m";
		case MILLISECONDS:
			return "ms";
		case SECONDS:
			return "s";
		default:
			throw new UnsupportedOperationException(tu.name());
		}
	}

	public DateHistogram monthly(int value) {
		interval = value + "M";
		return this;
	}

	public Aggregation secondly(int value) {
		interval = value + "s";
		return this;
	}

	@Override
	AggType type() {
		return AggType.date_histogram;
	}

	public DateHistogram weekly(int value) {
		interval = value + "w";
		return this;
	}

	public DateHistogram withInterval(int value, TimeUnit tu) {
		interval = String.format("%d%s", value, map(tu));

		return this;
	}

}
