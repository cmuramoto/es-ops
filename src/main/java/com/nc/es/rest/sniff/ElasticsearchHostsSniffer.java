package com.nc.es.rest.sniff;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.nc.es.rest.client.Response;
import com.nc.es.rest.client.RestClient;

/**
 * Class responsible for sniffing the http hosts from elasticsearch through the nodes info api and
 * returning them back. Compatible with elasticsearch 5.x and 2.x.
 */
public final class ElasticsearchHostsSniffer implements HostsSniffer {

	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchHostsSniffer.class);

	public static final long DEFAULT_SNIFF_REQUEST_TIMEOUT = TimeUnit.SECONDS.toMillis(1);

	private final RestClient restClient;
	private final Map<String, String> sniffRequestParams;
	private final Scheme scheme;
	private final JsonFactory jsonFactory = new JsonFactory();

	/**
	 * Creates a new instance of the Elasticsearch sniffer. It will use the provided
	 * {@link RestClient} to fetch the hosts, through the nodes info api, the default sniff request
	 * timeout value {@link #DEFAULT_SNIFF_REQUEST_TIMEOUT} and http as the scheme for all the
	 * hosts.
	 * 
	 * @param restClient
	 *            client used to fetch the hosts from elasticsearch through nodes info api. Usually
	 *            the same instance that is also provided to {@link Sniffer#builder(RestClient)}, so
	 *            that the hosts are set to the same client that was used to fetch them.
	 */
	public ElasticsearchHostsSniffer(RestClient restClient) {
		this(restClient, DEFAULT_SNIFF_REQUEST_TIMEOUT, ElasticsearchHostsSniffer.Scheme.HTTP);
	}

	/**
	 * Creates a new instance of the Elasticsearch sniffer. It will use the provided
	 * {@link RestClient} to fetch the hosts through the nodes info api, the provided sniff request
	 * timeout value and scheme.
	 * 
	 * @param restClient
	 *            client used to fetch the hosts from elasticsearch through nodes info api. Usually
	 *            the same instance that is also provided to {@link Sniffer#builder(RestClient)}, so
	 *            that the hosts are set to the same client that was used to sniff them.
	 * @param sniffRequestTimeoutMillis
	 *            the sniff request timeout (in milliseconds) to be passed in as a query string
	 *            parameter to elasticsearch. Allows to halt the request without any failure, as
	 *            only the nodes that have responded within this timeout will be returned.
	 * @param scheme
	 *            the scheme to associate sniffed nodes with (as it is not returned by
	 *            elasticsearch)
	 */
	public ElasticsearchHostsSniffer(RestClient restClient, long sniffRequestTimeoutMillis, Scheme scheme) {
		this.restClient = Objects.requireNonNull(restClient, "restClient cannot be null");
		if (sniffRequestTimeoutMillis < 0) {
			throw new IllegalArgumentException("sniffRequestTimeoutMillis must be greater than 0");
		}
		this.sniffRequestParams = Collections.<String, String> singletonMap("timeout", sniffRequestTimeoutMillis + "ms");
		this.scheme = Objects.requireNonNull(scheme, "scheme cannot be null");
	}

	/**
	 * Calls the elasticsearch nodes info api, parses the response and returns all the found http
	 * hosts
	 */
	public List<HttpHost> sniffHosts() throws IOException {
		Response response = restClient.performRequest("get", "/_nodes/http", sniffRequestParams);
		return readHosts(response.getEntity());
	}

	private List<HttpHost> readHosts(HttpEntity entity) throws IOException {
		try (InputStream inputStream = entity.getContent()) {
			JsonParser parser = jsonFactory.createParser(inputStream);
			if (parser.nextToken() != JsonToken.START_OBJECT) {
				throw new IOException("expected data to start with an object");
			}
			List<HttpHost> hosts = new ArrayList<>();
			while (parser.nextToken() != JsonToken.END_OBJECT) {
				if (parser.getCurrentToken() == JsonToken.START_OBJECT) {
					if ("nodes".equals(parser.getCurrentName())) {
						while (parser.nextToken() != JsonToken.END_OBJECT) {
							JsonToken token = parser.nextToken();
							assert token == JsonToken.START_OBJECT;
							String nodeId = parser.getCurrentName();
							HttpHost sniffedHost = readHost(nodeId, parser, this.scheme);
							if (sniffedHost != null) {
								if (LOG.isTraceEnabled()) {
									LOG.trace("adding node [{}]", nodeId);
								}
								hosts.add(sniffedHost);
							}
						}
					} else {
						parser.skipChildren();
					}
				}
			}
			return hosts;
		}
	}

	private static HttpHost readHost(String nodeId, JsonParser parser, Scheme scheme) throws IOException {
		HttpHost httpHost = null;
		String fieldName = null;
		while (parser.nextToken() != JsonToken.END_OBJECT) {
			if (parser.getCurrentToken() == JsonToken.FIELD_NAME) {
				fieldName = parser.getCurrentName();
			} else if (parser.getCurrentToken() == JsonToken.START_OBJECT) {
				if ("http".equals(fieldName)) {
					while (parser.nextToken() != JsonToken.END_OBJECT) {
						if (parser.getCurrentToken() == JsonToken.VALUE_STRING && "publish_address".equals(parser.getCurrentName())) {
							URI boundAddressAsURI = URI.create(scheme + "://" + parser.getValueAsString());
							httpHost = new HttpHost(boundAddressAsURI.getHost(), boundAddressAsURI.getPort(), boundAddressAsURI.getScheme());
						} else if (parser.getCurrentToken() == JsonToken.START_OBJECT) {
							parser.skipChildren();
						}
					}
				} else {
					parser.skipChildren();
				}
			}
		}
		// http section is not present if http is not enabled on the node, ignore such nodes
		if (httpHost == null) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("skipping node [{}] with http disabled", nodeId);
			}
			return null;
		}
		return httpHost;
	}

	public enum Scheme {
		HTTP("http"), HTTPS("https");

		private final String name;

		Scheme(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return name;
		}
	}
}
