package com.nc.es.api;

import com.nc.util.JsonSupport;

public interface IElasticSearchObject {

	default String asJson() {
		return JsonSupport.toJson(this);
	}

	default byte[] asJsonBytes() {
		return JsonSupport.toBytes(this);
	}

	default String asPrettyJson() {
		return JsonSupport.toPrettyJson(this);
	}
}