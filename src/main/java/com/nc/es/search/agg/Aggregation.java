package com.nc.es.search.agg;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.nc.es.api.IElasticSearchObject;
import com.nc.es.tuples.Tuple2;
import com.nc.es.tuples.Tuple4;

public abstract class Aggregation implements IElasticSearchObject {

	@JsonIgnore
	Tuple2<String, Aggregation> next;

	public Aggregation nest(String label, Aggregation agg) {
		next = Tuple2.of(label, agg);

		return this;
	}

	public Object rewrite(String label) {

		final Tuple2<AggType, Aggregation> root = Tuple2.of(type(), this);

		if (next != null) {
			return Tuple2.of(label, Tuple4.of(type().name(), this, "aggs", next.v.rewrite(next.k)));
		} else {
			return Tuple2.of(label, root);
		}
	}

	@Override
	public String toString() {
		return asPrettyJson();
	}

	abstract AggType type();
}
