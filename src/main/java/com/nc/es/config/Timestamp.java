package com.nc.es.config;

import com.fasterxml.jackson.annotation.JsonProperty;

@Deprecated
public class Timestamp {

	boolean enabled;
	@JsonProperty("ignore_missing")
	boolean ignoreMissing;

}
