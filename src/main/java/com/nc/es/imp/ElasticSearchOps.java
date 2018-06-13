package com.nc.es.imp;

import static com.nc.es.api.IElasticSearchOps.concat;
import static com.nc.es.api.IElasticSearchOps.slashed;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterators;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.StatusLine;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nc.es.api.ElasticSearchOperationException;
import com.nc.es.api.IElasticSearchOps;
import com.nc.es.api.NodeInfo;
import com.nc.es.config.IndexDefinition;
import com.nc.es.config.Mappings;
import com.nc.es.config.Settings;
import com.nc.es.ops.BulkDeleteResult;
import com.nc.es.ops.IBulkInsertResult;
import com.nc.es.ops.IBulkUpdateResult;
import com.nc.es.ops.ISearchResult;
import com.nc.es.ops.LookupDeserializer;
import com.nc.es.ops.MappedSearchResult;
import com.nc.es.ops.RefreshResult;
import com.nc.es.ops.Result;
import com.nc.es.ops.ScrollRequest;
import com.nc.es.ops.SimpleSearchResult;
import com.nc.es.ops.UpdateByQueryOptions;
import com.nc.es.ops.UpdateByQueryResult;
import com.nc.es.ops.UpdateStatement;
import com.nc.es.ops.Result.Status;
import com.nc.es.rest.client.Response;
import com.nc.es.rest.client.ResponseException;
import com.nc.es.rest.client.ResponseListener;
import com.nc.es.rest.client.RestClient;
import com.nc.es.rest.sniff.SniffOnFailureListener;
import com.nc.es.rest.sniff.Sniffer;
import com.nc.es.search.RootQuery;
import com.nc.es.tuples.Tuple2;
import com.nc.util.CollectionsHandler;
import com.nc.util.JsonSupport;

public class ElasticSearchOps implements IElasticSearchOps, AutoCloseable {

	protected class Bound implements IBound {

		final String index;
		final String type;
		final String endpoint;

		public Bound(final String index, final String type) {
			super();
			this.index = index;
			this.type = type;
			endpoint = String.format("/%s/%s", index, type);
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public String endpoint() {
			return endpoint;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public String index() {
			return index;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public String type() {
			return type;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public <T> ITypedBound<T> typed(final Class<T> binding) {
			return new ITypedBound<T>() {

				@Override
				public Class<T> kind() {
					return binding;
				}

				@Override
				public IBound wrapped() {
					return Bound.this;
				}
			};
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public IElasticSearchOps wrapped() {
			return ElasticSearchOps.this;
		}
	}

	static final class ScrollIterator<T> implements Iterator<T> {

		String scollId;
		int ttl;
		boolean ready;
		BiFunction<String, Integer, T> factory;
		T current;

		public ScrollIterator(final String scollId, final int ttl, final BiFunction<String, Integer, T> factory) {
			super();
			this.scollId = scollId;
			this.ttl = ttl;
			this.factory = factory;
		}

		private void fill() {
			final T v = factory.apply(scollId, ttl);
			ready = (current = v) != null;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean hasNext() {
			if (!ready) {
				fill();
			}

			return ready;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public T next() {
			if (!ready) {
				throw new IllegalStateException();
			}
			ready = false;
			final T rv = current;
			current = null;
			return rv;
		}

	}

	private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchOps.class);

	private static final String SCROLL_PATH = "/_search/scroll";

	private static final String BASE_SEARCH_PARAMS = "_search?filter_path=took,_shards,timed_out,hits.hits._source,hits.hits._id,hits.total,aggregations";

	private static final String SCROLL_SEARCH_PARAMS = "_search?filter_path=took,_shards,timed_out,hits.hits._source,hits.hits._id,hits.total,_scroll_id,&scroll=%ds";

	private static final String FILTER_SEARCH_PARAMS = "_search?filter_path=took,_shards,timed_out,hits.hits._id,hits.total,aggregations";

	private static final Map<String, String> NO_PARAMS = Collections.<String, String> emptyMap();

	static boolean is404(final Response r) {
		return r.getStatusLine().getStatusCode() == 404;
	}

	static String scriptDeleteFields(final String... fields) {
		final StringBuilder sb = new StringBuilder();
		for (final String field : fields) {
			sb.append(String.format("ctx._source.remove('%s');", field));
		}

		return sb.toString();
	}

	RestClient client;

	Sniffer sniffer;

	SniffOnFailureListener listener;

	boolean supportsTypes;

	public ElasticSearchOps(final HttpHost master, final int timeout) {
		this(master, timeout, true, true);
	}

	public ElasticSearchOps(final HttpHost master, final int timeout, final boolean sniff, final boolean sniffOnFailure) {
		this(new HttpHost[]{ master }, timeout, sniff, sniffOnFailure);
	}

	public ElasticSearchOps(final HttpHost[] hosts, final int timeout) {
		this(hosts, timeout, true, true);
	}

	public ElasticSearchOps(final HttpHost[] hosts, final int timeout, final boolean sniff, final boolean sniffOnFailure) {
		client = RestClient.builder(hosts).setMaxRetryTimeoutMillis(timeout <= 0 ? Integer.MAX_VALUE : timeout).build();
		if (sniff) {
			sniffer = Sniffer.builder(client).build();
			if (sniffOnFailure) {
				listener = new SniffOnFailureListener();
				listener.setSniffer(sniffer);
			}
		}

		supportsTypes = info().supportsTypes();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IBound bind(final String index, final String type) {
		return new Bound(index, maskType(type));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <T extends IBulkInsertResult, V> Stream<T> bulkInsert(final String endpoint, final Class<T> kind, final Stream<V> docs, final Function<V, String> idMapper, final BiConsumer<V, OutputStream> consumer, final int batch) {
		final BiConsumer<V, OutputStream> c = consumer == null ? JsonSupport::toJson : consumer;

		final String path = concat(endpoint, "_bulk");

		final byte[] pre = "{\"index\": {}}\n".getBytes(UTF_8);
		final int post = "\n".getBytes(UTF_8)[0];

		return CollectionsHandler.lazyPartition(docs, batch).map(s -> {
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			final long count = s.map(v -> {
				try {
					baos.write(idMapper == null ? pre : String.format("{\"index\":{\"_id\": \"%s\"}}\n", idMapper.apply(v)).getBytes(UTF_8));
					c.accept(v, baos);
					baos.write(post);
				} catch (final IOException e) {
					throw new InternalError(e);
				}
				return null;
			}).count();
			final byte[] chunk = baos.toByteArray();

			if (LOG.isDebugEnabled()) {
				LOG.debug("Bulk Inserting {} ({} bytes)", count, baos.size());
			}

			return doBulkOp(path, kind, new ByteArrayEntity(chunk, ContentType.APPLICATION_JSON));
		});
	}

	@Override
	public <T extends IBulkUpdateResult, X> Stream<T> bulkUpdate(final String endpoint, final Class<T> kind, final Stream<Tuple2<String, X>> docs, final Function<Tuple2<String, X>, UpdateStatement> factory, final int batch) {

		final String path = concat(endpoint, "_bulk");

		return CollectionsHandler.lazyPartition(docs, batch).map(s -> {
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			final long count = s.map(v -> {
				try {
					final UpdateStatement statement = factory.apply(v);
					statement.writeTo(v.k, baos);
				} catch (final IOException e) {
					throw new InternalError(e);
				}
				return null;
			}).count();
			final byte[] chunk = baos.toByteArray();

			if (LOG.isDebugEnabled()) {
				LOG.debug("Bulk Updating {} ({} bytes)", count, baos.size());
			}

			return doBulkOp(path, kind, new ByteArrayEntity(chunk, ContentType.APPLICATION_JSON));
		});
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() {
		AutoCloseable c = client;
		if (c != null) {
			try {
				c.close();
			} catch (final Exception e) {

			}
		}

		c = sniffer;

		if (c != null) {
			try {
				c.close();
			} catch (final Exception e) {

			}
		}
	}

	@Override
	public long count(final String endpoint, final RootQuery q) {
		try {
			HttpEntity he = new ByteArrayEntity(q.asJsonBytes(), ContentType.APPLICATION_JSON);
			final Response response = client.performRequest(POST_METHOD, concat(endpoint, "_count?filter_path=count"), NO_PARAMS, he);

			he = response.getEntity();
			try (InputStream is = he.getContent()) {
				@SuppressWarnings("unchecked")
				final Map<Object, Number> m = JsonSupport.fromJson(Map.class, is);

				return m.values().iterator().next().longValue();
			}
		} catch (final IOException ex) {
			throw new ElasticSearchOperationException(ex);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean createIndex(final String index, final IndexDefinition def) {
		final HttpEntity e = new ByteArrayEntity(def.asJsonBytes(), ContentType.APPLICATION_JSON);

		final CountDownLatch latch = new CountDownLatch(1);

		final AtomicBoolean result = new AtomicBoolean();

		final ResponseListener listener = new ResponseListener() {

			@Override
			public void onFailure(final Exception exception) {
				latch.countDown();
				exception.printStackTrace();
			}

			@Override
			public void onSuccess(final Response response) {
				result.set(true);
				latch.countDown();
				assert 200 == response.getStatusLine().getStatusCode();
			}
		};
		client.performRequestAsync(PUT_METHOD, slashed(index), NO_PARAMS, e, listener);

		try {
			latch.await();
		} catch (final InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new ElasticSearchOperationException(ex);
		}

		return result.get();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public BulkDeleteResult delete(final String endpoint, final RootQuery query) {
		HttpEntity e = new ByteArrayEntity(query.asJsonBytes(), ContentType.APPLICATION_JSON);
		try {
			final String path = concat(endpoint, "_delete_by_query?conflicts=proceed");
			final Response response = client.performRequest(POST_METHOD, path, NO_PARAMS, e);

			if (response.getStatusLine().getStatusCode() >= 400) {
				throw new IllegalStateException();
			}

			e = response.getEntity();
			try (InputStream is = e.getContent()) {
				return JsonSupport.fromJson(BulkDeleteResult.class, is);
			}
		} catch (final IOException ex) {
			throw new ElasticSearchOperationException(ex);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Result delete(final String endpoint, final String id) {
		try {
			final String path = concat(endpoint, id);
			final Response response = client.performRequest(DELETE_METHOD, path, NO_PARAMS);

			final HttpEntity e = response.getEntity();
			try (InputStream is = e.getContent()) {
				return JsonSupport.fromJson(Result.class, is);
			}

		} catch (final ResponseException re) {
			if (re.getResponse().getStatusLine().getStatusCode() == 404) {
				final Result rv = new Result();
				rv.result = Status.not_found;
				return rv;
			}
			throw new ElasticSearchOperationException(re);
		} catch (final IOException ex) {
			throw new ElasticSearchOperationException(ex);
		}
	}

	@Override
	public UpdateByQueryResult deleteFields(final String endpoint, final RootQuery rq, final UpdateByQueryOptions opts, final String... fields) {
		return updateByQuery(endpoint, rq.updating(scriptDeleteFields(fields)), opts);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean deleteIndex(final String index) {
		if (exists(index)) {
			try {
				final Response response = client.performRequest(DELETE_METHOD, slashed(index), NO_PARAMS);

				return response.getStatusLine().getStatusCode() == 200;
			} catch (final IOException e) {
				throw new ElasticSearchOperationException(e);
			}
		}

		return false;
	}

	private /* <T extends IBulkInsertResult> */ <T> T doBulkOp(final String path, final Class<T> kind, final HttpEntity e) {
		try {
			final Response response = client.performRequest(POST_METHOD, path, NO_PARAMS, e);

			final StatusLine line = response.getStatusLine();
			if (line.getStatusCode() >= 400) {
				try (InputStream is = response.getEntity().getContent()) {
					final byte[] chunk = new byte[is.available()];
					is.read(chunk);
					throw new ElasticSearchOperationException(new String(chunk), line);
				}
			}

			if (kind != null && !kind.isInterface() && !Modifier.isAbstract(kind.getModifiers())) {
				try (InputStream is = response.getEntity().getContent()) {
					return JsonSupport.fromJson(kind, is);
				}
			}
		} catch (final IOException ex) {
			throw new ElasticSearchOperationException(ex);
		}
		return null;
	}

	private Result doInsert(final String endpoint, HttpEntity e) {
		try {
			final Response response = client.performRequest(POST_METHOD, slashed(endpoint), NO_PARAMS, e);

			if (response.getStatusLine().getStatusCode() >= 400) {
				throw new IllegalStateException();
			}

			e = response.getEntity();
			try (InputStream is = e.getContent()) {
				return JsonSupport.fromJson(Result.class, is);
			}
		} catch (final IOException ex) {
			throw new ElasticSearchOperationException(ex);
		}
	}

	private Result doSaveOrUpdate(final String path, HttpEntity e, final String method) {
		try {
			final Response response = client.performRequest(method, path, NO_PARAMS, e);

			if (response.getStatusLine().getStatusCode() >= 400) {
				throw new ElasticSearchOperationException(response.getStatusLine());
			}

			e = response.getEntity();
			try (InputStream is = e.getContent()) {
				return JsonSupport.fromJson(Result.class, is);
			}
		} catch (final IOException ex) {
			throw new ElasticSearchOperationException(ex);
		}
	}

	private Result doSaveOrUpdate(final String endpoint, final String id, final HttpEntity e, final String method) {
		final String path = concat(endpoint, id);

		return doSaveOrUpdate(path, e, method);
	}

	private Result doSaveOrUpdate(final String index, final String type, final String id, final HttpEntity e, final String method) {
		final String path = String.format("/%s/%s/%s", index, maskType(type), id);
		return doSaveOrUpdate(path, e, method);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean exists(final String index) {
		try {
			final Response response = client.performRequest(HEAD_METHOD, slashed(index), Collections.<String, String> emptyMap());

			return response.getStatusLine().getStatusCode() == 200;
		} catch (final IOException e) {
			throw new ElasticSearchOperationException(e);
		}
	}

	String formatUrlPartialUpdate(final String endpoint, final String id) {
		return String.format("/%s/%s/_update", endpoint, id);
	}

	String formatUrlPartialUpdate(final String index, final String type, final String id) {
		return formatUrlPartialUpdate(String.format("%s/%s", index, maskType(type)), id);
	}

	<T> MappedSearchResult<T> head(final String endpoint, final Class<T> type, final RootQuery q, final String... fields) {
		final Function<Response, MappedSearchResult<T>> headFactory = response -> {
			try {
				final HttpEntity e = response.getEntity();
				try (InputStream is = e.getContent()) {
					return MappedSearchResult.deserialize(is, type);
				}
			} catch (final IOException ex) {
				throw new ElasticSearchOperationException(ex);
			}
		};

		final BiFunction<String, Integer, MappedSearchResult<T>> factory = (s, p) -> {
			final ScrollRequest req = new ScrollRequest(s, p);

			HttpEntity he = new ByteArrayEntity(req.asJsonBytes(), ContentType.APPLICATION_JSON);

			try {
				final Response response = client.performRequest(POST_METHOD, SCROLL_PATH, NO_PARAMS, he);

				he = response.getEntity();
				try (InputStream is = he.getContent()) {
					MappedSearchResult<T> rv = MappedSearchResult.deserialize(is, type);

					if (rv.hits() == null || rv.hits().isEmpty()) {
						rv = null;
					}
					return rv;
				}
			} catch (final IOException ex) {
				throw new ElasticSearchOperationException(ex);
			}
		};

		return scroll(endpoint, q, headFactory, factory, false, fields).findFirst().get();
	}

	@Override
	public NodeInfo info() {
		return info(client.any());
	}

	@Override
	public NodeInfo info(final HttpHost host) {
		Response res;
		try {
			res = client.performRequest(GET_METHOD, host.toString(), NO_PARAMS);
			final HttpEntity he = res.getEntity();

			try (InputStream is = he.getContent()) {
				return JsonSupport.fromJson(NodeInfo.class, is);
			}
		} catch (final IOException e) {
			throw new ElasticSearchOperationException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Result insert(final String endpoint, final byte[] json) {
		return doInsert(endpoint, new ByteArrayEntity(json, ContentType.APPLICATION_JSON));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Result insert(final String endpoint, final String json) {
		return doInsert(endpoint, new NStringEntity(json, ContentType.APPLICATION_JSON));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <T> T lookup(final String endpoint, final Class<T> type, final String id, final String... fields) {
		String path = singleProjection(id, fields);
		path = concat(endpoint, path);

		try {
			final Response response = client.performRequest(GET_METHOD, path, NO_PARAMS);

			if (is404(response)) {
				return null;
			}

			final HttpEntity he = response.getEntity();
			try (InputStream is = he.getContent()) {
				return LookupDeserializer.deserialize(type, is);
			}
		} catch (final IOException ex) {
			throw new ElasticSearchOperationException(ex);
		}
	}

	private <T> BiFunction<String, Integer, MappedSearchResult<T>> mappedScrollFactory(final Class<T> type) {
		return (s, p) -> {
			final ScrollRequest req = new ScrollRequest(s, p);

			HttpEntity he = new ByteArrayEntity(req.asJsonBytes(), ContentType.APPLICATION_JSON);

			try {
				final Response response = client.performRequest(POST_METHOD, SCROLL_PATH, NO_PARAMS, he);

				he = response.getEntity();
				try (InputStream is = he.getContent()) {
					MappedSearchResult<T> rv = MappedSearchResult.deserialize(is, type);

					if (rv.hits() == null || rv.hits().isEmpty()) {
						rv = null;
					}
					return rv;
				}
			} catch (final IOException ex) {
				throw new ElasticSearchOperationException(ex);
			}
		};
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Mappings mappings(final String index) {
		final String path = concat(index, "_mappings");

		try {
			final Response response = client.performRequest(GET_METHOD, path, NO_PARAMS);

			final HttpEntity he = response.getEntity();
			try (InputStream is = he.getContent()) {
				final byte[] chunk = new byte[is.available()];
				is.read(chunk, 0, chunk.length);

				@SuppressWarnings("unchecked")
				final Tuple2<String, Object> fromJson = JsonSupport.fromJson(Tuple2.class, chunk);
				return JsonSupport.fromJson(Mappings.class, fromJson.v);
			}
		} catch (final IOException ex) {
			throw new ElasticSearchOperationException(ex);
		}

	}

	private String maskType(final String type) {
		return supportsTypes ? type : "doc";
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <T> MappedSearchResult<T> next(final String endpoint, final Class<T> type, final String scrollId, final int ttl) {
		final ScrollIterator<MappedSearchResult<T>> itr = new ScrollIterator<>(scrollId, ttl, mappedScrollFactory(type));

		return itr.hasNext() ? itr.next() : null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public SimpleSearchResult next(final String endpoint, final String scrollId, final int ttl) {
		final ScrollIterator<SimpleSearchResult> itr = new ScrollIterator<>(scrollId, ttl, simpleScrollFactory());

		return itr.hasNext() ? itr.next() : null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Result partialUpdate(final String endpoint, final String id, final String json) {
		return doSaveOrUpdate(formatUrlPartialUpdate(endpoint, id), new NStringEntity(json, ContentType.APPLICATION_JSON), POST_METHOD);
	}

	@Override
	public Result partialUpdate(final String index, final String type, final String id, final byte[] json) {
		return doSaveOrUpdate(formatUrlPartialUpdate(index, type, id), new ByteArrayEntity(json, ContentType.APPLICATION_JSON), POST_METHOD);

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Result partialUpdate(final String index, final String type, final String id, final String json) {
		return doSaveOrUpdate(formatUrlPartialUpdate(index, type, id), new NStringEntity(json, ContentType.APPLICATION_JSON), POST_METHOD);
	}

	private String projectionPath(final String endpoint, final String[] fields, final int ttl) {
		final StringBuilder sb = new StringBuilder(endpoint.length() + BASE_SEARCH_PARAMS.length() + 8 * fields.length);
		sb.append(endpoint);
		if (!endpoint.endsWith("/")) {
			sb.append('/');
		}
		sb.append(FILTER_SEARCH_PARAMS).append(",hits.hits._source");

		if (ttl > 0) {
			sb.append(",_scroll_id&scroll=").append(ttl).append('s');
		}

		// better than specifying on filter path!
		int i = 0;
		sb.append("&_source_include=").append(fields[i++]);
		while (i < fields.length) {
			sb.append(",").append(fields[i++]);
		}

		return sb.toString();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <T> MappedSearchResult<T> query(final String endpoint, final Class<T> type, final RootQuery q, final String... fields) {
		HttpEntity e = new ByteArrayEntity(q.asJsonBytes(), ContentType.APPLICATION_JSON);
		final String path;
		if (fields.length == 0) {
			path = concat(endpoint, BASE_SEARCH_PARAMS);
		} else {
			path = projectionPath(endpoint, fields, 0);
		}

		try {
			final Response response = client.performRequest(POST_METHOD, path, NO_PARAMS, e);

			if (response.getStatusLine().getStatusCode() == 200) {
				e = response.getEntity();
				try (InputStream is = e.getContent()) {
					return MappedSearchResult.deserialize(is, type);
				}
			}

		} catch (final IOException ex) {
			throw new ElasticSearchOperationException(ex);
		}

		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public SimpleSearchResult query(final String endpoint, final RootQuery q, final String... fields) {
		HttpEntity e = new ByteArrayEntity(q.asJsonBytes(), ContentType.APPLICATION_JSON);
		final String path;
		if (fields.length == 0) {
			path = concat(endpoint, BASE_SEARCH_PARAMS);
		} else {
			path = projectionPath(endpoint, fields, 0);
		}

		try {
			final Response response = client.performRequest(POST_METHOD, path, NO_PARAMS, e);

			e = response.getEntity();
			try (InputStream is = e.getContent()) {
				return JsonSupport.fromJson(SimpleSearchResult.class, is);
			}
		} catch (final IOException ex) {
			throw new ElasticSearchOperationException(ex);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public SimpleSearchResult query(final String index, final String type, final RootQuery q, final String... fields) {
		return query(concat(index, maskType(type)), q);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public RefreshResult refresh(final String index) {
		try {
			final Response response = client.performRequest(POST_METHOD, concat(slashed(index), "_refresh"), NO_PARAMS);

			final HttpEntity e = response.getEntity();
			try (InputStream is = e.getContent()) {
				return JsonSupport.fromJson(RefreshResult.class, is);
			}
		} catch (final IOException ex) {
			throw new ElasticSearchOperationException(ex);
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Result saveOrUpdate(final String endpoint, final String id, final byte[] json) {
		return doSaveOrUpdate(endpoint, id, new ByteArrayEntity(json, ContentType.APPLICATION_JSON), PUT_METHOD);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Result saveOrUpdate(final String endpoint, final String id, final String json) {
		return doSaveOrUpdate(endpoint, id, new NStringEntity(json, ContentType.APPLICATION_JSON), PUT_METHOD);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Result saveOrUpdate(final String index, final String type, final String id, final byte[] json) {
		return doSaveOrUpdate(index, type, id, new ByteArrayEntity(json, ContentType.APPLICATION_JSON), PUT_METHOD);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Result saveOrUpdate(final String index, final String type, final String id, final String json) {
		return doSaveOrUpdate(index, type, id, new NStringEntity(json, ContentType.APPLICATION_JSON), PUT_METHOD);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <T> Stream<MappedSearchResult<T>> scroll(final String endpoint, final Class<T> type, final RootQuery q, final String... fields) {

		final Function<Response, MappedSearchResult<T>> headFactory = response -> {
			try {
				final HttpEntity e = response.getEntity();
				try (InputStream is = e.getContent()) {
					return MappedSearchResult.deserialize(is, type);
				}
			} catch (final IOException ex) {
				throw new ElasticSearchOperationException(ex);
			}
		};

		return scroll(endpoint, q, headFactory, mappedScrollFactory(type), true, fields);
	}

	private <T extends ISearchResult> Stream<T> scroll(final String endpoint, final RootQuery q, final Function<Response, T> headFactory, final BiFunction<String, Integer, T> factory, final boolean streaming, final String... fields) {
		final int ttl = q.scrollTtlOrDefault();

		final HttpEntity e = new ByteArrayEntity(q.asJsonBytes(), ContentType.APPLICATION_JSON);

		final String path;
		if (fields.length == 0) {
			path = concat(endpoint, String.format(SCROLL_SEARCH_PARAMS, ttl));
		} else {
			path = projectionPath(endpoint, fields, ttl);
		}

		T head;

		try {
			final Response response = client.performRequest(POST_METHOD, path, NO_PARAMS, e);

			head = headFactory.apply(response);
		} catch (final IOException ex) {
			throw new ElasticSearchOperationException(ex);
		}

		if (head.isEmptyOrComplete()) {
			return Stream.of(head);
		}

		if (streaming) {
			final Stream<T> stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(new ScrollIterator<>(head.scrollId(), ttl, factory), 0), false);

			return Stream.concat(Stream.of(head), stream);
		} else {
			return Stream.of(head);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Stream<SimpleSearchResult> scroll(final String endpoint, final RootQuery q, final String... fields) {

		final Function<Response, SimpleSearchResult> headFactory = response -> {
			try {
				final HttpEntity e = response.getEntity();
				try (InputStream is = e.getContent()) {
					return JsonSupport.fromJson(SimpleSearchResult.class, is);
				}
			} catch (final IOException ex) {
				throw new ElasticSearchOperationException(ex);
			}
		};

		return scroll(endpoint, q, headFactory, simpleScrollFactory(), true, fields);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Settings settings(final String index) {
		Response res;
		try {
			res = client.performRequest(GET_METHOD, concat(index, "_settings"), NO_PARAMS);
			final HttpEntity he = res.getEntity();

			try (InputStream is = he.getContent()) {
				HashMap<String, Object> m = JsonSupport.fromJson(HashMap.class, is);

				if (m != null //
						&& (m = (HashMap<String, Object>) m.get(index)) != null //
						&& (m = (HashMap<String, Object>) m.get("settings")) != null //
						&& (m = (HashMap<String, Object>) m.get("index")) != null) {
					return JsonSupport.mapper().convertValue(m, Settings.class);
				}

				return null;
			}
		} catch (final IOException e) {
			throw new ElasticSearchOperationException(e);
		}
	}

	BiFunction<String, Integer, SimpleSearchResult> simpleScrollFactory() {
		return (s, p) -> {
			final ScrollRequest req = new ScrollRequest(s, p);

			HttpEntity he = new ByteArrayEntity(req.asJsonBytes(), ContentType.APPLICATION_JSON);

			try {
				final Response response = client.performRequest(POST_METHOD, SCROLL_PATH, NO_PARAMS, he);

				he = response.getEntity();
				try (InputStream is = he.getContent()) {
					SimpleSearchResult rv = JsonSupport.fromJson(SimpleSearchResult.class, is);

					if (rv.hits() == null || rv.hits().isEmpty()) {
						rv = null;
					}
					return rv;
				}
			} catch (final IOException ex) {
				throw new ElasticSearchOperationException(ex);
			}
		};
	}

	private String singleProjection(final String id, final String[] fields) {
		if (fields == null || fields.length == 0) {
			return id + "?filter_path=_source";
		} else {
			final StringBuilder sb = new StringBuilder(id).append("?filter_path=");
			final Iterator<String> iterator = Arrays.asList(fields).iterator();
			sb.append("_source.").append(iterator.next());

			while (iterator.hasNext()) {
				sb.append(",").append("_source.").append(iterator.next());
			}

			return sb.toString();
		}
	}

	@Override
	public boolean supportsTypes() {
		return supportsTypes;
	}

	@Override
	public UpdateByQueryResult updateByQuery(final String endpoint, final RootQuery query, final UpdateByQueryOptions opts) {
		HttpEntity e = new ByteArrayEntity(query.asJsonBytes(), ContentType.APPLICATION_JSON);
		try {
			final String op = opts == null ? null : opts.toQueryString();
			final String p = op == null ? "_update_by_query?conflicts=proceed" : String.format("_update_by_query?%s", op);
			final String path = concat(endpoint, p);
			final Response response = client.performRequest(POST_METHOD, path, NO_PARAMS, e);

			if (response.getStatusLine().getStatusCode() >= 400) {
				throw new IllegalStateException();
			}

			e = response.getEntity();
			try (InputStream is = e.getContent()) {
				return JsonSupport.fromJson(UpdateByQueryResult.class, is);
			}
		} catch (final IOException ex) {
			throw new ElasticSearchOperationException(ex);
		}
	}

}