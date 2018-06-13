package com.nc.es.ops;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.nc.es.api.IElasticSearchObject;

public class Result implements IElasticSearchObject {

	public static enum Status {
		created, updated, deleted, not_found, noop;
	}

	public Status result;

	@JsonProperty("_shards")
	public ShardStats shards;

	@JsonProperty("_index")
	public String index;

	@JsonProperty("_type")
	public String type;

	@JsonProperty("_id")
	public String id;

	@JsonProperty("_version")
	public int version;

	// public boolean found;

	// public boolean created;

	@Override
	public String toString() {
		return asPrettyJson();
	}

}
