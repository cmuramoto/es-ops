package com.nc.es.ops;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
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
import com.nc.util.JsonSupport;

/**
 * Statefull scrolling.
 *
 * @author cmuramoto
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = RestClientConfig.class)
public class TestStatefullScroll {

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

		final AtomicInteger pages = new AtomicInteger(0);
		final AtomicInteger docs = new AtomicInteger(0);
		final Set<Integer> ids = new HashSet<>();

		MappedSearchResult<Value> res = bound.scroll(Value.class, RootQuery.matchAll().scrolling(pageSize)).findFirst().get();

		do {
			docs.addAndGet(res.hits().size());
			pages.incrementAndGet();

			res.vals().forEach(v -> {
				ids.add(v.id);
				Assert.assertEquals("FOO", v.a);
				Assert.assertEquals("BAR", v.b);
			});
		} while ((res = bound.next(Value.class, res.scrollId, 60)) != null);

		Assert.assertEquals(total, docs.get());

		Assert.assertEquals(expectedRoundTrips, pages.get());

		Assert.assertEquals(total, ids.size());

		IntStream.range(0, total).forEach(id -> Assert.assertTrue(ids.contains(id)));

		bound.delete(RootQuery.matchAll());

		bound.refresh();
	}

	@Test
	public void runRaw() {
		final int expectedRoundTrips = total / pageSize + (total % pageSize == 0 ? 0 : 1);

		final IBound bound = ops.bind("test", "fake");

		bound.bulkInsertRaw(IBulkInsertResult.Shallow.class, IntStream.range(0, total).mapToObj(i -> JsonSupport.toJson(Collections.singletonMap("NM_AIRLINE_DESIGNATOR", "FAKE_DESIGNATOR_OF_FAKE_TESTS"))), 1000).forEach(s -> {
			Assert.assertFalse(s.errors());
		});

		bound.refresh();
		final AtomicInteger pages = new AtomicInteger(0);
		final AtomicInteger docs = new AtomicInteger(0);

		SimpleSearchResult res = bound.scroll(RootQuery.matchAll().scrolling(pageSize)).findFirst().get();

		do {
			res.hits().stream().forEach(h -> {
				Assert.assertEquals("FAKE_DESIGNATOR_OF_FAKE_TESTS", h.source.get("NM_AIRLINE_DESIGNATOR"));
			});
			docs.addAndGet(res.hits().size());
			pages.incrementAndGet();
		} while ((res = bound.next(res.scrollId, 60)) != null);

		Assert.assertEquals(total, docs.get());

		Assert.assertEquals(expectedRoundTrips, pages.get());

		bound.delete(RootQuery.matchAll());

		bound.refresh();
	}

	@Before
	public void setup() {
		ops.deleteIfExists("test");
		log.info("Creating index");

		final IndexDefinition def = IndexDefinition.get().with(Mappings.get().add("doc", IndexedType.get().disableAll().add("NM_AIRLINE_DESIGNATOR", Property.keyword().with(IndexOptions.docs))));
		ops.createIndex("test", def);
	}

}
