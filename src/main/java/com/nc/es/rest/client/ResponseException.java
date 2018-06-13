package com.nc.es.rest.client;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.util.EntityUtils;

/**
 * Exception thrown when an elasticsearch node responds to a request with a status code that
 * indicates an error. Holds the response that was returned.
 */
public final class ResponseException extends IOException {

	private static final long serialVersionUID = 1L;
	private Response response;

	public ResponseException(Response response) throws IOException {
		super(buildMessage(response));
		this.response = response;
	}

	private static String buildMessage(Response response) throws IOException {
		String message = response.getRequestLine().getMethod() + " " + response.getHost() + response.getRequestLine().getUri() + ": " + response.getStatusLine().toString();

		HttpEntity entity = response.getEntity();
		if (entity != null) {
			if (entity.isRepeatable() == false) {
				entity = new BufferedHttpEntity(entity);
				response.getHttpResponse().setEntity(entity);
			}
			message += "\n" + EntityUtils.toString(entity);
		}
		return message;
	}

	/**
	 * Returns the {@link Response} that caused this exception to be thrown.
	 */
	public Response getResponse() {
		return response;
	}
}
