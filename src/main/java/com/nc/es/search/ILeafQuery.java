package com.nc.es.search;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.nc.es.api.ElasticSearchOperationException;
import com.nc.es.tuples.Tuple2;
import com.nc.es.tuples.Tuple4;
import com.nc.util.JavaTime;

public interface ILeafQuery extends IQuery {

	public static class Exists implements ILeafQuery {

		public static Exists exists(final String field) {
			return new Exists(field);
		}

		String field;

		Exists() {
		}

		public Exists(final String field) {
			super();
			this.field = field;
		}

		@Override
		public Kind kind() {
			return Kind.exists;
		}

	}

	public static class Ids implements ILeafQuery {

		public static Ids ids(final Collection<String> ids) {
			return of("doc").appendOrSet(ids);
		}

		public static Ids ids(final Iterable<String> ids) {
			return of("doc").append(ids);
		}

		public static Ids ids(final String... ids) {
			return of("doc").append(ids);
		}

		public static Ids of(final String type) {
			final Ids ids = new Ids();
			ids.type = type;

			return ids;
		}

		String type;

		Collection<String> values;

		public Ids append(final Collection<String> ids) {
			Collection<String> vals = values;
			if (vals == null) {
				vals = values = new ArrayList<>(ids.size());
			}

			vals.addAll(ids);

			return this;
		}

		public Ids append(final Iterable<String> ids) {
			Collection<String> vals = values;
			if (vals == null) {
				vals = values = new ArrayList<>();
			}

			for (final String id : ids) {
				vals.add(id);
			}

			return this;
		}

		public Ids append(final String... ids) {
			Collection<String> vals = values;
			if (vals == null) {
				vals = values = new ArrayList<>();
			}

			for (final String id : ids) {
				vals.add(id);
			}

			return this;
		}

		public Ids appendOrSet(final Collection<String> ids) {
			if (values == null) {
				values = ids;
			} else {
				append(ids);
			}
			return this;
		}

		@Override
		public Kind kind() {
			return Kind.ids;
		}

	}

	public static class Match implements ILeafQuery {

		public static Match query(final String field, final String text) {
			final Match rv = new Match();
			final Map<String, Object> map = new HashMap<>(2);
			map.put("query", text);
			rv.m = Tuple2.of(field, map);

			return rv;
		}

		public static Match simple(final String field, final String text) {
			final Match rv = new Match();
			rv.m = Tuple2.of(field, text);

			return rv;
		}

		Tuple2<String, Object> m;

		public Match analyzer(final String which) {
			map().put("analyzer", which);

			return this;
		}

		@Override
		public Kind kind() {
			return Kind.match;
		}

		@SuppressWarnings("unchecked")
		private Map<String, Object> map() {
			if (m == null || !(m.v instanceof Map)) {
				throw new ElasticSearchOperationException("");
			}

			return (Map<String, Object>) m.v;
		}

		@Override
		public Object rewrite() {
			return m;
		}

		public Match useAnd() {
			map().put("operator", "and");
			return this;
		}

		public Match useOr() {
			map().put("operator", "or");
			return this;
		}

	}

	public static class Range implements ILeafQuery {

		public static Range range(final String term) {
			final Range r = new Range();
			r.field = term;

			return r;
		}

		@JsonIgnore
		String field;

		Object lt;
		Object lte;
		Object gt;
		Object gte;

		Float boost;
		String format;
		String time_zone;

		@JsonIgnore
		DateTimeFormatter fmt;

		public Range exact(final LocalDate val) {
			return lte(val).gte(val);
		}

		public Range exact(final LocalDateTime val) {
			return lte(val).gte(val);
		}

		public Range exact(final Object val) {
			return lte(val).gte(val);
		}

		String format(final LocalDate date) {
			return date == null ? "now" : fmt == null ? date.toString() : fmt.format(date);
		}

		String format(final LocalDateTime time) {
			return time == null ? "now" : fmt == null ? time.toString() : fmt.format(time);
		}

		public Range format(final String pattern) {
			format = pattern;

			if (pattern != null) {
				fmt = DateTimeFormatter.ofPattern(pattern);
			}

			return this;
		}

		public Range gt(final LocalDate time) {
			return gt(format(time));
		}

		public Range gt(final LocalDateTime time) {
			return gt(format(time));
		}

		public Range gt(final Object val) {
			gt = val;
			gte = null;
			return this;
		}

		public Range gte(final LocalDate time) {
			return gte(format(time));
		}

		public Range gte(final LocalDateTime time) {
			return gte(format(time));
		}

		public Range gte(final Object val) {
			gte = val;
			gt = null;
			return this;
		}

		public Range isoDate() {
			return format(JavaTime.ISO_DATE_PATTERN);
		}

		public Range isoDateTime() {
			return format(JavaTime.ISO_NO_ZONE_DATE_TIME_PATTERN);
		}

		public Range isoTime() {
			return format(JavaTime.ISO_NO_ZONE_TIME_PATTERN);
		}

		@Override
		public Kind kind() {
			return Kind.range;
		}

		public Range lt(final LocalDate time) {
			return lt(format(time));
		}

		public Range lt(final LocalDateTime time) {
			return lt(format(time));
		}

		public Range lt(final Object val) {
			lt = val;
			lte = null;
			return this;
		}

		public Range lte(final LocalDate time) {
			return lte(format(time));
		}

		public Range lte(final LocalDateTime time) {
			return lte(format(time));
		}

		public Range lte(final Object val) {
			lte = val;
			lt = null;
			return this;
		}

		@Override
		public Object rewrite() {
			return Tuple2.of(field, this);
		}
	}

	public static class Term implements ILeafQuery {

		public static Term term(final String term, final String value) {
			final Term rv = new Term();
			rv.term = Tuple2.of(term, value);

			return rv;
		}

		@JsonIgnore
		Tuple2<String, String> term;

		Float boost;

		@Override
		public Kind kind() {
			return Kind.term;
		}

		@Override
		public Object rewrite() {
			return boost == null ? term : Tuple4.of(term.k, term.v, "boost", boost);
		}
	}

	public static class Terms<T> implements ILeafQuery {

		public static <T, L extends Collection<T>> Terms<T> terms(final String term, final L values) {
			final Terms<T> rv = new Terms<>();
			rv.terms = Tuple2.of(term, values);

			return rv;
		}

		@JsonIgnore
		Tuple2<String, Collection<T>> terms;

		@Override
		public Kind kind() {
			return Kind.terms;
		}

		@Override
		public Object rewrite() {
			return terms;
		}
	}

	public static class WildCard implements ILeafQuery {

		public static WildCard wildcard(final String term, final String value) {
			final WildCard wc = new WildCard();
			wc.term = term;
			wc.value = value;

			return wc;
		}

		String term;

		String value;

		@Override
		public Kind kind() {
			return Kind.wildcard;
		}

		@Override
		public Object rewrite() {
			return Tuple2.of(term, value);
		}
	}
}
