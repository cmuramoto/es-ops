package com.nc.es.index;

import com.fasterxml.jackson.annotation.JsonProperty;

public class IndexRequest {

	public static IndexRequest standard() {
		return new IndexRequest();
	}

	@JsonProperty("_index")
	String index;
	@JsonProperty("_type")
	String type;
	@JsonProperty("_id")
	String id;

}
