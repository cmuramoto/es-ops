package com.nc.es.search;

import com.nc.es.api.IElasticSearchObject;
import com.nc.es.tuples.Tuple2;

public interface IQuery extends IElasticSearchObject {

	public enum Kind {
		// leaves
		term, terms, range, exists, prefix, wildcard, regexp, fuzzy, type, ids,
		// full-text (leaves)
		match, match_phrase, match_phrase_prefix, multi_match, common, query_string, simple_query_string,
		// compound
		constant_score, bool, dis_max, function_score, boosting,
		// join
		nested, has_child, has_parent, parent_id,
		// special
		more_like_this, template, script, percolate;
	}

	public default RootQuery asRoot() {
		final RootQuery rq = new RootQuery();
		rq.query = Tuple2.of(kind(), rewrite());

		return rq;
	}

	public Kind kind();

	public default String kindName() {
		return kind().name();
	}

	public default Object rewrite() {
		return this;
	}

}
