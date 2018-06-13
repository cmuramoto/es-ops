package com.nc.es.search.agg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.nc.es.api.IElasticSearchObject;
import com.nc.es.tuples.Tuple2;
import com.nc.util.JsonSupport;

public class Bucket implements IElasticSearchObject {

	public static void main(String[] args) {
		final Bucket root = new Bucket();

		final Bucket head = new Bucket();
		head.key_as_string = "2017-02-22T16:33:32.000";
		head.key = 1487792012000L;
		head.doc_count = 2L;
		head.buckets = new ArrayList<>();
		final Bucket e = new Bucket();
		e.doc_count_error_upper_bound = 0;
		e.sum_other_doc_count = 0;
		e.key = "127.0.0.1";
		e.doc_count = 2l;
		head.sub = Tuple2.of("XX", e);

		root.buckets = new ArrayList<>();
		root.buckets.add(head);

		final Map<String, Object> m = new HashMap<>();
		m.put("aggregations", Tuple2.of("AA", root));

		System.out.println(JsonSupport.toPrettyJson(m));
	}

	String key_as_string;
	Object key;
	Long doc_count;
	Integer doc_count_error_upper_bound;
	Integer sum_other_doc_count;
	List<Bucket> buckets;

	Map<String, Bucket> sub;

	public long docs() {
		return doc_count == null ? 0 : doc_count;
	}

	public Integer keyAsInt() {
		return key instanceof Integer ? (Integer) key : key instanceof Number ? ((Number) key).intValue() : null;
	}

	public Long keyAsLong() {
		return key instanceof Long ? (Long) key : key instanceof Number ? ((Number) key).longValue() : null;
	}

	public String keyAsString() {
		return key_as_string;
	}

	@Override
	public String toString() {
		return asPrettyJson();
	}
}
