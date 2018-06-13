package br.atech.commons.es;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.nc.es.api.IElasticSearchOps;
import com.nc.es.api.IElasticSearchOps.IBound;
import com.nc.es.config.DynamicTemplate;
import com.nc.es.config.IndexDefinition;
import com.nc.es.config.IndexedType;
import com.nc.es.config.Mappings;
import com.nc.es.config.Property;
import com.nc.es.config.Property.IndexOptions;
import com.nc.es.ops.MappedSearchResult;
import com.nc.es.ops.Result;
import com.nc.es.ops.SimpleSearchResult;
import com.nc.es.ops.Result.Status;
import com.nc.es.search.RootQuery;
import com.nc.es.tuples.Tuple4;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = RestClientConfig.class)
public class TestQuery {

	public static class ProjectionOne extends ESObject {
		@JsonProperty("UNMAPPED_ATTR")
		String attr;

	}

	public static class ProjectionTwo extends ESObject {

		@JsonProperty("NM_AIRLINE_DESIGNATOR")
		String designator;
		@JsonProperty("UNMAPPED_ATTR")
		String attr;

	}

	@Autowired
	IElasticSearchOps ops;

	@Autowired
	Logger log;

	@Test
	public void run() {

		final IBound bound = ops.bind("test", "fake");

		final Result res = bound.insert(Collections.singletonMap("NM_AIRLINE_DESIGNATOR", "FAKE_DESIGNATOR_OF_FAKE_TESTS"));

		log.info("{}", res);

		Assert.assertSame(Status.created, res.result);

		Assert.assertSame(Status.created, bound.insert(Tuple4.of("NM_AIRLINE_DESIGNATOR", "FAKE_DESIGNATOR_OF_FAKE_TESTS", "UNMAPPED_ATTR", "DYNAMIC1")).result);
		Assert.assertSame(Status.created, bound.insert(Tuple4.of("NM_AIRLINE_DESIGNATOR", "FAKE_DESIGNATOR_OF_FAKE_TESTS", "UNMAPPED_ATTR", "DYNAMIC2")).result);
		Assert.assertSame(Status.created, bound.insert(Tuple4.of("NM_AIRLINE_DESIGNATOR", "FAKE_DESIGNATOR_OF_FAKE_TESTS", "UNMAPPED_ATTR", "DYNAMIC3")).result);
		Assert.assertSame(Status.created, bound.insert(Tuple4.of("NM_AIRLINE_DESIGNATOR", "FAKE_DESIGNATOR_OF_FAKE_TESTS", "UNMAPPED_ATTR", "DYNAMIC4")).result);

		bound.refresh();

		final SimpleSearchResult result = bound.query(RootQuery.filter("NM_AIRLINE_DESIGNATOR", "FAKE_DESIGNATOR_OF_FAKE_TESTS"));

		Assert.assertEquals(5, result.total());

		final Set<Object> dVals = result.hits().stream().filter(h -> h.source != null && h.source.containsKey("UNMAPPED_ATTR")).map(h -> h.source.get("UNMAPPED_ATTR")).collect(Collectors.toSet());

		Assert.assertEquals(4, dVals.size());
		Assert.assertTrue(dVals.contains("DYNAMIC1"));
		Assert.assertTrue(dVals.contains("DYNAMIC2"));
		Assert.assertTrue(dVals.contains("DYNAMIC3"));
		Assert.assertTrue(dVals.contains("DYNAMIC4"));

		final MappedSearchResult<ProjectionOne> mrOne = bound.query(ProjectionOne.class, RootQuery.filter("NM_AIRLINE_DESIGNATOR", "FAKE_DESIGNATOR_OF_FAKE_TESTS"), "UNMAPPED_ATTR");

		Set<String> vals = mrOne.vals().map(v -> v.attr).filter(v -> v != null).collect(Collectors.toSet());
		Assert.assertEquals(4, vals.size());
		Assert.assertTrue(vals.contains("DYNAMIC1"));
		Assert.assertTrue(vals.contains("DYNAMIC2"));
		Assert.assertTrue(vals.contains("DYNAMIC3"));
		Assert.assertTrue(vals.contains("DYNAMIC4"));

		MappedSearchResult<ProjectionTwo> mrTwo = bound.query(ProjectionTwo.class, RootQuery.filter("NM_AIRLINE_DESIGNATOR", "FAKE_DESIGNATOR_OF_FAKE_TESTS"), "NM_AIRLINE_DESIGNATOR", "UNMAPPED_ATTR");
		vals = mrTwo.vals().map(v -> v.attr).filter(v -> v != null).collect(Collectors.toSet());
		Assert.assertEquals(4, vals.size());
		Assert.assertTrue(vals.contains("DYNAMIC1"));
		Assert.assertTrue(vals.contains("DYNAMIC2"));
		Assert.assertTrue(vals.contains("DYNAMIC3"));
		Assert.assertTrue(vals.contains("DYNAMIC4"));

		vals = mrTwo.vals().map(v -> v.designator).filter(v -> v != null).collect(Collectors.toSet());
		Assert.assertEquals(1, vals.size());
		Assert.assertTrue(vals.contains("FAKE_DESIGNATOR_OF_FAKE_TESTS"));

		mrTwo = bound.query(ProjectionTwo.class, RootQuery.filter("NM_AIRLINE_DESIGNATOR", "FAKE_DESIGNATOR_OF_FAKE_TESTS"), "NM_AIRLINE_DESIGNATOR");
		vals = mrTwo.vals().map(v -> v.attr).filter(v -> v != null).collect(Collectors.toSet());
		Assert.assertEquals(0, vals.size());

	}

	@Before
	public void setup() {
		if (ops.exists("test")) {
			log.info("Deleting index");
			ops.deleteIndex("test");
		}
		log.info("Creating index");

		final IndexDefinition def = IndexDefinition.get().with(Mappings.get().add("doc", IndexedType.get().disableAll().add("NM_AIRLINE_DESIGNATOR", Property.keyword().with(IndexOptions.docs)).with("non_analyzed", DynamicTemplate.keywordDefaults())));
		ops.createIndex("test", def);
	}
}