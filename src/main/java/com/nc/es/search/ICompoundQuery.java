package com.nc.es.search;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import com.nc.es.tuples.Tuple2;

public interface ICompoundQuery extends IQuery {

	public static class Bool implements ICompoundQuery {

		public static Bool create() {
			return new Bool();
		}

		List<Tuple2<Kind, Object>> must;

		List<Tuple2<Kind, Object>> must_not;

		List<Tuple2<Kind, Object>> filter;

		List<Tuple2<Kind, Object>> should;

		Integer minimum_should_match;

		Float boost;

		void append(List<Tuple2<Kind, Object>> target, Stream<IQuery> source) {
			final Iterator<IQuery> iterator = source.iterator();
			while (iterator.hasNext()) {
				final IQuery query = iterator.next();
				target.add(Tuple2.of(query.kind(), query.rewrite()));
			}
		}

		private List<Tuple2<Kind, Object>> filter() {
			List<Tuple2<Kind, Object>> filter = this.filter;
			if (filter == null) {
				this.filter = filter = new ArrayList<>(1);
			}
			return filter;
		}

		public Bool filter(IQuery... queries) {
			final List<Tuple2<Kind, Object>> filter = filter();
			for (final IQuery q : queries) {
				filter.add(Tuple2.of(q.kind(), q.rewrite()));
			}

			return this;
		}

		public Bool filter(Stream<IQuery> queries) {
			append(filter(), queries);

			return this;
		}

		@Override
		public Kind kind() {
			return Kind.bool;
		}

		private List<Tuple2<Kind, Object>> must() {
			List<Tuple2<Kind, Object>> must = this.must;
			if (must == null) {
				this.must = must = new ArrayList<>(1);
			}
			return must;
		}

		public Bool must(IQuery... queries) {
			final List<Tuple2<Kind, Object>> must = must();

			for (final IQuery q : queries) {
				must.add(Tuple2.of(q.kind(), q.rewrite()));
			}
			return this;
		}

		public Bool must(Stream<IQuery> queries) {
			append(must(), queries);

			return this;
		}

		List<Tuple2<Kind, Object>> must_not() {

			List<Tuple2<Kind, Object>> must_not = this.must_not;
			if (must_not == null) {
				this.must_not = must_not = new ArrayList<>();
			}
			return must_not;
		}

		public Bool mustNot(IQuery... queries) {
			final List<Tuple2<Kind, Object>> must_not = must_not();

			for (final IQuery q : queries) {
				must_not.add(Tuple2.of(q.kind(), q.rewrite()));
			}

			return this;
		}

		public Bool mustNot(Stream<IQuery> queries) {
			append(must_not(), queries);

			return this;
		}

		private List<Tuple2<Kind, Object>> should() {
			List<Tuple2<Kind, Object>> should = this.should;
			if (should == null) {
				this.should = should = new ArrayList<>();
			}
			return should;
		}

		public Bool should(IQuery... queries) {
			final List<Tuple2<Kind, Object>> should = should();

			for (final IQuery q : queries) {
				should.add(Tuple2.of(q.kind(), q.rewrite()));
			}

			return this;

		}

		public Bool should(Stream<IQuery> queries) {
			append(should(), queries);

			return this;
		}
	}

	public static class ConstantScore implements ICompoundQuery {

		public static ConstantScore constant(IQuery q) {
			final ConstantScore cs = new ConstantScore();

			cs.filter = Tuple2.of(q.kind(), q.rewrite());

			return cs;
		}

		Object filter;

		Float boost;

		@Override
		public Kind kind() {
			return Kind.constant_score;
		}
	}

}