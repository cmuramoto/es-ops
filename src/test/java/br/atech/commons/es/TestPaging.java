package br.atech.commons.es;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.nc.es.api.IElasticSearchOps;
import com.nc.es.api.IElasticSearchOps.IBound;
import com.nc.es.config.IndexDefinition;
import com.nc.es.config.IndexedType;
import com.nc.es.config.Mappings;
import com.nc.es.config.Property;
import com.nc.es.config.Property.IndexOptions;
import com.nc.es.ops.IBulkInsertResult;
import com.nc.es.ops.MappedSearchResult;
import com.nc.es.ops.SimpleSearchResult;
import com.nc.es.search.RootQuery;
import com.nc.es.search.Sort;
import com.nc.util.JsonSupport;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = RestClientConfig.class)
public class TestPaging {

	static class Value {

		int id;

		String a;

		String b;

	}

	@Autowired
	IElasticSearchOps ops;

	@Autowired
	Logger log;

	int pageSize = 17;
	int total = 200;

	@Test
	public void runMapped() {
		final int expectedRoundTrips = total / pageSize + (total % pageSize == 0 ? 0 : 1);

		final IBound bound = ops.bind("test", "fake");

		bound.bulkInsert(IBulkInsertResult.Shallow.class, IntStream.range(0, total).mapToObj(i -> {
			final Value v = new Value();
			v.id = i;
			v.a = "FOO";
			v.b = "BAR";
			return v;
		}), 1000).forEach(s -> {
			Assert.assertFalse(s.errors());
		});

		bound.refresh();

		final List<Integer> ids = new ArrayList<>();

		IntStream.range(0, expectedRoundTrips).forEach(p -> {
			final RootQuery q = RootQuery.matchAll().paging(p * pageSize, pageSize).orderBy("id", Sort.asc().missingLast());
			final MappedSearchResult<Value> result = bound.query(Value.class, q, "id");

			if (p == expectedRoundTrips - 1) {
				LoggerFactory.getLogger(getClass()).info("Last page (expected: {})", total % pageSize);

				Assert.assertEquals(total % pageSize, result.size());

			} else {
				Assert.assertEquals(pageSize, result.size());
			}

			ids.addAll(result.vals().map(v -> v.id).collect(Collectors.toList()));

		});

		final TreeSet<Integer> ts = new TreeSet<>();
		ts.addAll(ids);

		Assert.assertArrayEquals(ts.toArray(), ids.toArray());
	}

	@Test
	public void runRaw() {
		final int expectedRoundTrips = total / pageSize + (total % pageSize == 0 ? 0 : 1);

		final IBound bound = ops.bind("test", "fake");

		bound.bulkInsertRaw(IBulkInsertResult.Shallow.class, IntStream.range(0, total).mapToObj(i -> JsonSupport.toJson(Collections.singletonMap("NM_AIRLINE_DESIGNATOR", "FAKE_DESIGNATOR_OF_FAKE_TESTS"))), 1000).forEach(s -> {
			Assert.assertFalse(s.errors());
		});

		bound.refresh();

		IntStream.range(0, expectedRoundTrips).forEach(p -> {
			final SimpleSearchResult result = bound.query(RootQuery.matchAll().paging(p * pageSize, pageSize));

			if (p == expectedRoundTrips - 1) {
				Assert.assertEquals(total % pageSize, result.size());
			} else {
				Assert.assertEquals(pageSize, result.size());
			}
		});
	}

	@Before
	public void setup() {
		if (ops.exists("test")) {
			log.info("Deleting index");
			ops.deleteIndex("test");
		}
		log.info("Creating index");

		final IndexDefinition def = IndexDefinition.get().with(Mappings.get().add("doc", IndexedType.get().disableAll().add("NM_AIRLINE_DESIGNATOR", Property.keyword().with(IndexOptions.docs))));
		ops.createIndex("test", def);
	}

}
