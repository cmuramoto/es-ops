package com.nc.es.ops;

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
import com.nc.es.search.RootQuery;
import com.nc.es.search.Sort;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = RestClientConfig.class)
public class TestBounded {

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
		final IBound bound = ops.bind("test", "doc");

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

		final RootQuery q = RootQuery.matchAll().limit(pageSize).orderBy("id", Sort.asc().missingLast());
		final MappedSearchResult<Value> result = bound.query(Value.class, q);

		Assert.assertEquals(pageSize, result.size());
	}

	@Before
	public void setup() {
		if (ops.exists("test")) {
			log.info("Deleting index");
			ops.deleteIndex("test");
		}
		log.info("Creating index");

		final IndexDefinition def = IndexDefinition.get().with(Mappings.get().withDefaults(IndexedType.get().disableAll().add("NM_AIRLINE_DESIGNATOR", Property.keyword().with(IndexOptions.docs))));
		ops.createIndex("test", def);
	}

}
