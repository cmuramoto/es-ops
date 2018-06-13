package com.nc.es.api;

import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.http.HttpHost;

import com.nc.es.config.IndexDefinition;
import com.nc.es.config.Mappings;
import com.nc.es.config.Settings;
import com.nc.es.ops.BulkDeleteResult;
import com.nc.es.ops.IBulkInsertResult;
import com.nc.es.ops.IBulkUpdateResult;
import com.nc.es.ops.MappedSearchResult;
import com.nc.es.ops.RefreshResult;
import com.nc.es.ops.Result;
import com.nc.es.ops.SimpleSearchResult;
import com.nc.es.ops.UpdateByQueryOptions;
import com.nc.es.ops.UpdateByQueryResult;
import com.nc.es.ops.UpdateStatement;
import com.nc.es.search.RootQuery;
import com.nc.es.tuples.Tuple2;
import com.nc.util.JsonSupport;

public interface IElasticSearchOps {

	public interface IBound {

		default <T extends IBulkInsertResult, V> Stream<T> bulkInsert(final Class<T> kind, final Stream<V> docs, final BiConsumer<V, OutputStream> consumer, final int batch) {
			return wrapped().bulkInsert(endpoint(), kind, docs, consumer, batch);
		}

		default <T extends IBulkInsertResult, V> Stream<T> bulkInsert(final Class<T> kind, final Stream<V> docs, final Function<V, String> idMapper, final int batch) {
			return wrapped().bulkInsert(endpoint(), kind, docs, idMapper, batch);
		}

		default <T extends IBulkInsertResult, V> Stream<T> bulkInsert(final Class<T> kind, final Stream<V> docs, final int batch) {
			return wrapped().bulkInsert(endpoint(), kind, docs, batch);
		}

		default <V> IBulkInsertResult.Shallow bulkInsert(final Stream<V> docs, final BiConsumer<V, OutputStream> consumer, final int batch) {
			return bulkInsert(docs, null, consumer, batch);
		}

		default <V> IBulkInsertResult.Shallow bulkInsert(final Stream<V> docs, final Function<V, String> idMapper, final BiConsumer<V, OutputStream> consumer, final int batch) {
			return wrapped().bulkInsert(endpoint(), docs, idMapper, consumer, batch);
		}

		default <V> IBulkInsertResult.Shallow bulkInsert(final Stream<V> docs, final Function<V, String> idMapper, final int batch) {
			return bulkInsert(docs, idMapper, null, batch);
		}

		default <V> IBulkInsertResult.Shallow bulkInsert(final Stream<V> docs, final int batch) {
			return bulkInsert(docs, null, null, batch);
		}

		default <T extends IBulkInsertResult> Stream<T> bulkInsertRaw(final Class<T> kind, final Stream<String> docs, final int batch) {
			return wrapped().bulkInsertRaw(endpoint(), kind, docs, batch);
		}

		default <T extends IBulkUpdateResult, X> Stream<T> bulkUpdate(final Class<T> kind, final Stream<Tuple2<String, X>> docs, final Function<Tuple2<String, X>, UpdateStatement> factory, final int batch) {
			return wrapped().bulkUpdate(endpoint(), kind, docs, factory, batch);
		}

		default <T extends IBulkUpdateResult, X> Stream<T> bulkUpdate(final Class<T> kind, final Stream<Tuple2<String, X>> docs, final int batch) {
			return wrapped().bulkUpdate(endpoint(), kind, docs, batch);
		}

		default <T extends IBulkUpdateResult, X> Stream<T> bulkUpdate(final Class<T> kind, final Stream<X> docs, final Function<X, String> objToId, final Function<Tuple2<String, X>, UpdateStatement> factory, final int batch) {

			return wrapped().bulkUpdate(endpoint(), kind, docs, objToId, factory, batch);
		}

		default <X> IBulkUpdateResult.Shallow bulkUpdate(final Stream<Tuple2<String, X>> docs, final Function<Tuple2<String, X>, UpdateStatement> factory, final int batch) {
			return wrapped().bulkUpdate(endpoint(), docs, factory, batch);
		}

		default <X> IBulkUpdateResult.Shallow bulkUpdate(final Stream<Tuple2<String, X>> docs, final int batch) {
			return wrapped().bulkUpdate(endpoint(), docs, batch);
		}

		default long count(final RootQuery q) {
			return wrapped().count(endpoint(), q);
		}

		default BulkDeleteResult delete(final RootQuery query) {
			return wrapped().delete(endpoint(), query);
		}

		default Result delete(final String id) {
			return wrapped().delete(endpoint(), id);
		}

		default UpdateByQueryResult deleteFields(final RootQuery rq, final String... fields) {
			return deleteFields(rq, null, fields);
		}

		default UpdateByQueryResult deleteFields(final RootQuery rq, final UpdateByQueryOptions options, final String... fields) {
			return wrapped().deleteFields(endpoint(), rq, options, fields);
		}

		String endpoint();

		default boolean exists() {
			return wrapped().exists(index());
		}

		String index();

		default <T> Result insert(final byte[] json) {
			return wrapped().insert(endpoint(), json);
		}

		default Result insert(final String json) {
			return wrapped().insert(endpoint(), json);
		}

		default <T> Result insert(final T json) {
			return wrapped().insert(endpoint(), JsonSupport.toBytes(json));
		}

		default <T> T lookup(final Class<T> type, final String id, final String... fields) {
			return wrapped().lookup(endpoint(), type, id, fields);
		}

		default <T> MappedSearchResult<T> next(final Class<T> type, final String scrollId, final int ttl) {
			return wrapped().next(endpoint(), type, scrollId, ttl);
		}

		default SimpleSearchResult next(final String scrollId, final int ttl) {
			return wrapped().next(endpoint(), scrollId, ttl);
		}

		default <T> Result partialUpdate(final String id, final byte[] json) {
			return wrapped().partialUpdate(index(), type(), id, json);
		}

		default <T> Result partialUpdate(final String id, final String json) {
			return wrapped().partialUpdate(index(), type(), id, json);
		}

		default <T> Result partialUpdate(final String id, final T json) {
			return wrapped().partialUpdate(index(), type(), id, json);
		}

		default <T> MappedSearchResult<T> query(final Class<T> type, final RootQuery q, final String... fields) {
			return wrapped().query(endpoint(), type, q, fields);
		}

		default SimpleSearchResult query(final RootQuery q, final String... fields) {
			return wrapped().query(endpoint(), q, fields);
		}

		default RefreshResult refresh() {
			return wrapped().refresh(index());
		}

		default <T> Result saveOrUpdate(final String id, final T json) {
			return wrapped().saveOrUpdate(index(), type(), id, JsonSupport.toBytes(json));
		}

		default <T> Stream<MappedSearchResult<T>> scroll(final Class<T> type, final RootQuery q, final String... fields) {
			return wrapped().scroll(endpoint(), type, q, fields);
		}

		default Stream<SimpleSearchResult> scroll(final RootQuery q, final String... fields) {
			return wrapped().scroll(endpoint(), q, fields);
		}

		String type();

		<T> ITypedBound<T> typed(Class<T> binding);

		default UpdateByQueryResult updateByQuery(final RootQuery rq) {
			return updateByQuery(rq, null);
		}

		default UpdateByQueryResult updateByQuery(final RootQuery rq, final UpdateByQueryOptions options) {
			return wrapped().updateByQuery(endpoint(), rq, options);
		}

		IElasticSearchOps wrapped();
	}

	public interface ITypedBound<T> {

		default Result insert(final T t) {
			return wrapped().insert(t);
		}

		Class<T> kind();

		default T lookup(final String id, final String... fields) {
			return wrapped().lookup(kind(), id, fields);
		}

		default MappedSearchResult<T> next(final String scrollId, final int ttl) {
			return wrapped().next(kind(), scrollId, ttl);
		}

		default MappedSearchResult<T> query(final RootQuery q, final String... fields) {
			return wrapped().query(kind(), q, fields);
		}

		default Stream<MappedSearchResult<T>> scroll(final RootQuery q, final String... fields) {
			return wrapped().scroll(kind(), q, fields);
		}

		IBound wrapped();
	}

	String DELETE_METHOD = "DELETE";

	String GET_METHOD = "GET";

	String HEAD_METHOD = "HEAD";

	String POST_METHOD = "POST";

	String PUT_METHOD = "PUT";

	Charset UTF_8 = Charset.forName("UTF-8");

	int LINE_BREAK = '\n';

	static String concat(final String l, final String r) {
		return l.endsWith("/") ? l + r : String.format("%s/%s", l, r);
	}

	static String concat(final String l, final String r, final String s) {
		return l.endsWith("/") ? r.endsWith("/") ? l + r + s : String.format("%s%s/%s", l, r, s) : r.endsWith("/") ? String.format("%s/%s%s", l, r, s) : String.format("%s/%s/%s", l, r, s);
	}

	static String slashed(final String s) {
		return s.startsWith("/") ? s : "/" + s;
	}

	default IBound bind(final String index) {
		return bind(index, "doc");
	}

	IBound bind(String index, String type);

	default <T extends IBulkInsertResult, V> Stream<T> bulkInsert(final String endpoint, final Class<T> kind, final Stream<V> docs, final BiConsumer<V, OutputStream> consumer, final int batch) {
		return bulkInsert(endpoint, kind, docs, null, consumer, batch);
	}

	<T extends IBulkInsertResult, V> Stream<T> bulkInsert(String endpoint, Class<T> kind, Stream<V> docs, Function<V, String> idMapper, BiConsumer<V, OutputStream> consumer, int batch);

	default <T extends IBulkInsertResult, V> Stream<T> bulkInsert(final String endpoint, final Class<T> kind, final Stream<V> docs, final Function<V, String> idMapper, final int batch) {
		return bulkInsert(endpoint, kind, docs, idMapper, null, batch);
	}

	default <T extends IBulkInsertResult, V> Stream<T> bulkInsert(final String endpoint, final Class<T> kind, final Stream<V> docs, final int batch) {
		return bulkInsert(endpoint, kind, docs, null, null, batch);
	}

	default <V> IBulkInsertResult.Shallow bulkInsert(final String endpoint, final Stream<V> docs, final BiConsumer<V, OutputStream> consumer, final int batch) {
		return bulkInsert(endpoint, docs, null, consumer, batch);
	}

	default <V> IBulkInsertResult.Shallow bulkInsert(final String endpoint, final Stream<V> docs, final Function<V, String> idMapper, final BiConsumer<V, OutputStream> consumer, final int batch) {
		return bulkInsert(endpoint, IBulkInsertResult.Shallow.class, docs, idMapper, consumer, batch).reduce(IBulkInsertResult.Shallow::merge).orElse(null);
	}

	default <V> IBulkInsertResult.Shallow bulkInsert(final String endpoint, final Stream<V> docs, final Function<V, String> idMapper, final int batch) {
		return bulkInsert(endpoint, docs, idMapper, null, batch);
	}

	default <V> IBulkInsertResult.Shallow bulkInsert(final String endpoint, final Stream<V> docs, final int batch) {
		return bulkInsert(endpoint, docs, null, null, batch);
	}

	default <T extends IBulkInsertResult, V> Stream<T> bulkInsert(final String index, final String type, final Class<T> kind, final Stream<V> docs, final int batch) {
		return bulkInsert(toEndpoint(index, type), kind, docs, batch);
	}

	default <T extends IBulkInsertResult> Stream<T> bulkInsertRaw(final String endpoint, final Class<T> kind, final Stream<String> docs, final int batch) {
		final BiConsumer<String, OutputStream> bc = (s, o) -> {
			try {
				o.write(s.getBytes(UTF_8));
			} catch (final Exception e) {
				throw new ElasticSearchOperationException(e);
			}
		};

		return bulkInsert(endpoint, kind, docs, bc, batch);
	}

	default <T extends IBulkInsertResult> Stream<T> bulkInsertRaw(final String index, final String type, final Class<T> kind, final Stream<String> docs, final int batch) {
		return bulkInsertRaw(toEndpoint(index, type), kind, docs, batch);
	}

	<T extends IBulkUpdateResult, X> Stream<T> bulkUpdate(String endpoint, Class<T> kind, Stream<Tuple2<String, X>> docs, Function<Tuple2<String, X>, UpdateStatement> factory, int batch);

	default <T extends IBulkUpdateResult, X> Stream<T> bulkUpdate(final String endpoint, final Class<T> kind, final Stream<Tuple2<String, X>> docs, final int batch) {
		return bulkUpdate(endpoint, kind, docs, UpdateStatement::doc, batch);
	}

	default <T extends IBulkUpdateResult, X> Stream<T> bulkUpdate(final String endpoint, final Class<T> kind, final Stream<X> docs, final Function<X, String> objToId, final Function<Tuple2<String, X>, UpdateStatement> factory, final int batch) {

		return bulkUpdate(endpoint, kind, docs.map(d -> Tuple2.of(objToId.apply(d), d)), factory, batch);
	}

	default <X> IBulkUpdateResult.Shallow bulkUpdate(final String endpoint, final Stream<Tuple2<String, X>> docs, final Function<Tuple2<String, X>, UpdateStatement> factory, final int batch) {
		return bulkUpdate(endpoint, IBulkUpdateResult.Shallow.class, docs, factory, batch).reduce(IBulkUpdateResult.Shallow::merge).orElse(null);
	}

	default <X> IBulkUpdateResult.Shallow bulkUpdate(final String endpoint, final Stream<Tuple2<String, X>> docs, final int batch) {
		return bulkUpdate(endpoint, docs, UpdateStatement::doc, batch);
	}

	long count(String endpoint, RootQuery q);

	boolean createIndex(String index, IndexDefinition def);

	BulkDeleteResult delete(String endpoint, RootQuery query);

	Result delete(String endpoint, String id);

	default BulkDeleteResult delete(final String index, final String type, final RootQuery query) {
		return delete(String.format("/%s/%s", index, type), query);
	}

	default Result delete(final String index, final String type, final String id) {
		return delete(String.format("/%s/%s", index, type), id);
	}

	default UpdateByQueryResult deleteFields(final String endpoint, final RootQuery rq, final String... fields) {
		return deleteFields(endpoint, rq, null, fields);
	}

	UpdateByQueryResult deleteFields(String endpoint, RootQuery rq, UpdateByQueryOptions opts, String... fields);

	default boolean deleteIfExists(final String index) {
		if (exists(index)) {
			return deleteIndex(index);
		}
		return false;
	}

	boolean deleteIndex(String index);

	boolean exists(String index);

	NodeInfo info();

	NodeInfo info(HttpHost host);

	Result insert(String endpoint, byte[] json);

	Result insert(String endpoint, String json);

	default Result insert(final String index, final String type, final String json) {
		return insert(String.format("/%s/%s", index, type), json);
	}

	default <T> Result insert(final String index, final String type, final T value) {
		return insert(index, type, JsonSupport.toBytes(value));
	}

	default <T> Result insert(final String endpoint, final T json) {
		return insert(endpoint, JsonSupport.toBytes(json));
	}

	<T> T lookup(String endpoint, Class<T> type, String id, String... fields);

	Mappings mappings(String index);

	<T> MappedSearchResult<T> next(String endpoint, Class<T> type, String scrollId, int ttl);

	SimpleSearchResult next(String endpoint, String scrollId, int ttl);

	Result partialUpdate(String endpoint, String id, String json);

	Result partialUpdate(String index, String type, String id, byte[] json);

	Result partialUpdate(String index, String type, String id, String json);

	default <T> Result partialUpdate(final String index, final String type, final String id, final T json) {
		return partialUpdate(index, type, id, JsonSupport.toBytes(Tuple2.of("doc", json)));
	}

	<T> MappedSearchResult<T> query(String endpoint, Class<T> type, RootQuery q, String... fields);

	SimpleSearchResult query(String endpoint, RootQuery q, String... fields);

	default <T> MappedSearchResult<T> query(final String index, final String type, final Class<T> clazz, final RootQuery q, final String... fields) {
		return query(toEndpoint(index, type), clazz, q, fields);
	}

	default SimpleSearchResult query(final String index, final String type, final RootQuery q, final String... fields) {
		return query(toEndpoint(index, type), q, fields);
	}

	RefreshResult refresh(String index);

	Result saveOrUpdate(String endpoint, String id, byte[] json);

	Result saveOrUpdate(String endpoint, String id, String json);

	Result saveOrUpdate(String index, String type, String id, byte[] json);

	Result saveOrUpdate(String index, String type, String id, String json);

	default <T> Result saveOrUpdate(final String index, final String type, final String id, final T json) {
		return saveOrUpdate(index, type, id, JsonSupport.toBytes(json));
	}

	default <T> Result saveOrUpdate(final String endpoint, final String id, final T json) {
		return saveOrUpdate(endpoint, id, JsonSupport.toBytes(json));
	}

	<T> Stream<MappedSearchResult<T>> scroll(String endpoint, Class<T> type, RootQuery q, String... fields);

	Stream<SimpleSearchResult> scroll(String endpoint, RootQuery q, String... fields);

	Settings settings(String index);

	boolean supportsTypes();

	default String toEndpoint(final String index, final String type) {
		return concat(slashed(index), supportsTypes() ? type : "doc");
	}

	default UpdateByQueryResult updateByQuery(final String endpoint, final RootQuery rq) {
		return updateByQuery(endpoint, rq, null);
	}

	UpdateByQueryResult updateByQuery(String endpoint, RootQuery rq, UpdateByQueryOptions opts);
}