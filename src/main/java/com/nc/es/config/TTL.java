package com.nc.es.config;

@Deprecated
public class TTL {

	public static TTL get() {
		final TTL ttl = new TTL();
		ttl.enable = true;
		return ttl;
	}

	boolean enable;

}