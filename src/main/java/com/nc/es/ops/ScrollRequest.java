package com.nc.es.ops;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nc.es.api.IElasticSearchObject;

public class ScrollRequest implements IElasticSearchObject {

	@JsonProperty("scroll_id")
	String scrollId;

	@JsonIgnore
	int ttl;

	public ScrollRequest() {
	}

	public ScrollRequest(String scrollId, int ttl) {
		this.scrollId = scrollId;
		this.ttl = ttl;
	}

	@JsonProperty("scroll")
	public String scrollTTL() {
		return String.format("%ds", ttl);
	}

}
