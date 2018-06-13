package com.nc.es.rest.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.RequestLine;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class that exposes static methods to unify the way requests are logged. Includes trace
 * logging to log complete requests and responses in curl format. Useful for debugging, manually
 * sending logged requests via curl and checking their responses. Trace logging is a feature that
 * all the language clients provide.
 */
final class RequestLogger {

	private static final Logger TRACER = LoggerFactory.getLogger(RequestLogger.class);

	private RequestLogger() {
	}

	/**
	 * Logs a request that yielded a response
	 */
	static void logResponse(Logger logger, HttpUriRequest request, HttpHost host, HttpResponse httpResponse) {
		if (logger.isDebugEnabled()) {
			logger.debug("request [" + request.getMethod() + " " + host + getUri(request.getRequestLine()) + "] returned [" + httpResponse.getStatusLine() + "]");
		}
		if (TRACER.isTraceEnabled()) {
			String requestLine;
			try {
				requestLine = buildTraceRequest(request, host);
			} catch (IOException e) {
				requestLine = "";
				TRACER.trace("error while reading request for trace purposes", e);
			}
			String responseLine;
			try {
				responseLine = buildTraceResponse(httpResponse);
			} catch (IOException e) {
				responseLine = "";
				TRACER.trace("error while reading response for trace purposes", e);
			}
			TRACER.trace(requestLine + '\n' + responseLine);
		}
	}

	/**
	 * Logs a request that failed
	 */
	static void logFailedRequest(Logger logger, HttpUriRequest request, HttpHost host, Exception e) {
		if (logger.isDebugEnabled()) {
			logger.debug("request [" + request.getMethod() + " " + host + getUri(request.getRequestLine()) + "] failed", e);
		}
		if (TRACER.isTraceEnabled()) {
			String traceRequest;
			try {
				traceRequest = buildTraceRequest(request, host);
			} catch (IOException e1) {
				TRACER.trace("error while reading request for trace purposes", e);
				traceRequest = "";
			}
			TRACER.trace(traceRequest);
		}
	}

	/**
	 * Creates curl output for given request
	 */
	static String buildTraceRequest(HttpUriRequest request, HttpHost host) throws IOException {
		String requestLine = "curl -iX " + request.getMethod() + " '" + host + getUri(request.getRequestLine()) + "'";
		if (request instanceof HttpEntityEnclosingRequest) {
			HttpEntityEnclosingRequest enclosingRequest = (HttpEntityEnclosingRequest) request;
			if (enclosingRequest.getEntity() != null) {
				requestLine += " -d '";
				HttpEntity entity = enclosingRequest.getEntity();
				if (entity.isRepeatable() == false) {
					entity = new BufferedHttpEntity(enclosingRequest.getEntity());
					enclosingRequest.setEntity(entity);
				}
				requestLine += EntityUtils.toString(entity, StandardCharsets.UTF_8) + "'";
			}
		}
		return requestLine;
	}

	/**
	 * Creates curl output for given response
	 */
	static String buildTraceResponse(HttpResponse httpResponse) throws IOException {
		String responseLine = "# " + httpResponse.getStatusLine().toString();
		for (Header header : httpResponse.getAllHeaders()) {
			responseLine += "\n# " + header.getName() + ": " + header.getValue();
		}
		responseLine += "\n#";
		HttpEntity entity = httpResponse.getEntity();
		if (entity != null) {
			if (entity.isRepeatable() == false) {
				entity = new BufferedHttpEntity(entity);
			}
			httpResponse.setEntity(entity);
			ContentType contentType = ContentType.get(entity);
			Charset charset = StandardCharsets.UTF_8;
			if (contentType != null) {
				charset = contentType.getCharset();
			}
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent(), charset))) {
				String line;
				while ((line = reader.readLine()) != null) {
					responseLine += "\n# " + line;
				}
			}
		}
		return responseLine;
	}

	private static String getUri(RequestLine requestLine) {
		if (requestLine.getUri().charAt(0) != '/') {
			return "/" + requestLine.getUri();
		}
		return requestLine.getUri();
	}
}
