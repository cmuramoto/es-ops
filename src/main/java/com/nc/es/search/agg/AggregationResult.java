package com.nc.es.search.agg;

import java.util.Map;

import com.nc.es.ops.ShardStats;

public class AggregationResult {

	int took;

	boolean timed_out;

	ShardStats _shards;

	Map<String, Bucket> aggregations;

}
