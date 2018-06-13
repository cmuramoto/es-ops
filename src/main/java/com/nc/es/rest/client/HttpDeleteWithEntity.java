package com.nc.es.rest.client;

import java.net.URI;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

/**
 * Allows to send DELETE requests providing a body (not supported out of the box)
 */
final class HttpDeleteWithEntity extends HttpEntityEnclosingRequestBase {

	static final String METHOD_NAME = HttpDelete.METHOD_NAME;

	HttpDeleteWithEntity(final URI uri) {
		setURI(uri);
	}

	@Override
	public String getMethod() {
		return METHOD_NAME;
	}
}
