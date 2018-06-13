package com.nc.es.ops;

import com.nc.es.api.IElasticSearchObject;

public class BulkOperationResult implements IElasticSearchObject {

	public int took;

	public boolean timed_out;

	public int updated;

	public int deleted;

	public int batches;

	public int version_conflicts;

	public int noops;

	Retries retries;

	public int throttled_millis;

	public float requests_per_second;

	public int throttled_until_millis;

	public int total;

	public String task;

	@Override
	public String toString() {
		return asPrettyJson();
	}
}
