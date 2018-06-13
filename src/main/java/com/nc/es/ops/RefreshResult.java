package com.nc.es.ops;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RefreshResult {

	@JsonProperty("_shards")
	public ShardStats shards;

}
