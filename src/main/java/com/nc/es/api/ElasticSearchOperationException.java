package com.nc.es.api;

import org.apache.http.StatusLine;

public class ElasticSearchOperationException extends RuntimeException implements IElasticSearchObject {

	private static final long serialVersionUID = 1L;

	static final boolean SUPPRESS_STACK_TRACE = Boolean.getBoolean("com.nc.es.api.SUPPRESS_STACK_TRACE");

	StatusLine line;

	public ElasticSearchOperationException(StatusLine line) {
		this("", line);
	}

	public ElasticSearchOperationException(String message) {
		this(message, (Throwable) null);
	}

	public ElasticSearchOperationException(String message, StatusLine line) {
		this(message, (Throwable) null);
		this.line = line;
	}

	public ElasticSearchOperationException(String message, Throwable cause) {
		super(message, cause, SUPPRESS_STACK_TRACE, !SUPPRESS_STACK_TRACE);
	}

	public ElasticSearchOperationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public ElasticSearchOperationException(Throwable cause) {
		this("", cause);
	}

	// @Override
	// public String toString() {
	// final LinkedList<Throwable> causes = new LinkedList<>();
	// causes.add(this);
	//
	// Throwable root = getCause();
	// if (root != null) {
	// do {
	// causes.add(root);
	// } while ((root = root.getCause()) != null && !causes.contains(root));
	// }
	//
	// return asPrettyJson();
	// }

}
