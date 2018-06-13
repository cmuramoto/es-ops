package com.nc.es.search;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nc.es.api.IElasticSearchObject;
import com.nc.es.ops.UpdateStatement;
import com.nc.es.search.ICompoundQuery.ConstantScore;
import com.nc.es.search.ILeafQuery.Range;
import com.nc.es.search.ILeafQuery.Term;
import com.nc.es.search.ILeafQuery.WildCard;
import com.nc.es.search.agg.Aggregation;
import com.nc.es.tuples.Tuple2;
import com.nc.util.Empty;

public class RootQuery implements IElasticSearchObject {

	public static RootQuery dateHigh(final String term, final LocalDateTime high, final String pattern) {
		final DateTimeFormatter fmt = DateTimeFormatter.ofPattern(pattern);

		return Range.range(term).gte(high == null ? "now" : high.format(fmt)).format(pattern).asRoot();
	}

	public static RootQuery dateLow(final String term, final LocalDateTime low, final String pattern) {
		final DateTimeFormatter fmt = DateTimeFormatter.ofPattern(pattern);

		return Range.range(term).lte(low == null ? "now" : low.format(fmt)).format(pattern).asRoot();
	}

	public static RootQuery dateRange(final String term, final LocalDateTime low, final LocalDateTime high, final String pattern) {
		final DateTimeFormatter fmt = DateTimeFormatter.ofPattern(pattern);

		return Range.range(term).lte(low == null ? "now" : low.format(fmt)).gte(high == null ? "now" : high.format(fmt)).format(pattern).asRoot();
	}

	public static RootQuery equal(final String term, final long val) {
		return Range.range(term).exact(val).asRoot();
	}

	public static RootQuery filter(final String term, final String value) {
		return ConstantScore.constant(Term.term(term, value)).asRoot();
	}

	public static RootQuery greaterThanOrEqual(final String term, final long val) {
		return Range.range(term).gte(val).asRoot();
	}

	public static RootQuery lowerThanOrEqual(final String term, final long val) {
		return Range.range(term).lte(val).asRoot();
	}

	public static RootQuery matchAll() {
		final RootQuery rv = new RootQuery();
		rv.query = Collections.singletonMap("match_all", new Empty());
		return rv;
	}

	public static RootQuery range(final String term, final long low, final long high) {
		return Range.range(term).gte(low).lt(high).asRoot();
	}

	public static RootQuery term(final String term, final String value) {
		final Term t = Term.term(term, value);

		return t.asRoot();
	}

	public static RootQuery wildcard(final String term, final String value) {
		return WildCard.wildcard(term, value).asRoot();
	}

	public Integer from;

	public Integer size;

	@JsonIgnore
	public Integer scrollTTL;

	@JsonProperty("_source")
	public Boolean source;

	UpdateStatement.Script script;

	Object query;

	List<Tuple2<String, Sort>> sort;

	Object aggs;

	public RootQuery aggregate(final String label, final Aggregation agg) {
		aggs = agg.rewrite(label);
		return this;
	}

	public RootQuery limit(final int size) {
		this.size = size;
		return this;
	}

	public RootQuery orderBy(final String term, final Sort s) {
		List<Tuple2<String, Sort>> sort = this.sort;
		if (sort == null) {
			this.sort = sort = new ArrayList<>();
		}
		sort.add(Tuple2.of(term, s));

		return this;
	}

	public int pageSizeOrDefault() {
		final Integer sz = size;
		return sz == null || sz < 1 ? 100 : sz.intValue();
	}

	public RootQuery paging(final int from, final int size) {
		this.from = from;
		this.size = size;

		return this;
	}

	public RootQuery scrolling(final int size) {
		return scrolling(size, 60);
	}

	public RootQuery scrolling(final int size, final int scrollTTL) {
		this.size = size;
		this.scrollTTL = scrollTTL;
		return this;
	}

	public int scrollTtlOrDefault() {
		final Integer ttl = scrollTTL;
		return ttl == null || ttl.intValue() < 1 ? 60 : ttl;
	}

	@Override
	public String toString() {
		return asPrettyJson();
	}

	public RootQuery updating(final String source) {
		return updating(source, null, null);
	}

	public RootQuery updating(final String source, final Map<String, Object> params) {
		return updating(source, null, params);
	}

	public RootQuery updating(final String source, final String lang) {
		return updating(source, lang, null);
	}

	public RootQuery updating(final String source, final String lang, final Map<String, Object> params) {
		script = new UpdateStatement.Script();
		script.source = source;
		script.lang = lang;
		script.params = params;

		return this;
	}
}