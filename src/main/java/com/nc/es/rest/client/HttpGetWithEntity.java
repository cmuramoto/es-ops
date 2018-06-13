package com.nc.es.rest.client;

import java.net.URI;

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;

/**
 * Allows to send GET requests providing a body (not supported out of the box)
 */
final class HttpGetWithEntity extends HttpEntityEnclosingRequestBase {

	static final String METHOD_NAME = HttpGet.METHOD_NAME;

	HttpGetWithEntity(final URI uri) {
		setURI(uri);
	}

	@Override
	public String getMethod() {
		return METHOD_NAME;
	}
}