package com.nc.es.index;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BulkRequest {

	@JsonProperty("index")
	IndexRequest index;

}
