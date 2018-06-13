package com.nc.es.rest.client;

/**
 * Listener to be provided when calling async performRequest methods provided by {@link RestClient}.
 * Those methods that do accept a listener will return immediately, execute asynchronously, and
 * notify the listener whenever the request yielded a response, or failed with an exception.
 */
public interface ResponseListener {

	/**
	 * Method invoked if the request yielded a successful response
	 */
	void onSuccess(Response response);

	/**
	 * Method invoked if the request failed. There are two main categories of failures: connection
	 * failures (usually {@link java.io.IOException}s, or responses that were treated as errors
	 * based on their error response code ({@link ResponseException}s).
	 */
	void onFailure(Exception exception);
}