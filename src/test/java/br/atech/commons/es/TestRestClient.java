package br.atech.commons.es;

import java.util.Collections;

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
import com.nc.es.config.Property.Type;
import com.nc.es.ops.BulkDeleteResult;
import com.nc.es.ops.RefreshResult;
import com.nc.es.ops.Result;
import com.nc.es.ops.Result.Status;
import com.nc.es.search.RootQuery;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = RestClientConfig.class)
public class TestRestClient {

	@Autowired
	IElasticSearchOps ops;

	@Autowired
	Logger log;

	@Test
	public void run() {

		final IBound bound = ops.bind("test", "fake");

		Result res = bound.insert(Collections.singletonMap("NM_AIRLINE_DESIGNATOR", "FAKE_DESIGNATOR_OF_FAKE_TESTS"));

		log.info("{}", res);

		Assert.assertSame(res.result, Status.created);

		res = bound.insert(Collections.singletonMap("NM_AIRLINE_DESIGNATOR", "FAKE_DESIGNATOR_OF_FAKE_TESTS"));
		res = bound.insert(Collections.singletonMap("NM_AIRLINE_DESIGNATOR", "FAKE_DESIGNATOR_OF_FAKE_TESTS"));
		res = bound.insert(Collections.singletonMap("NM_AIRLINE_DESIGNATOR", "FAKE_DESIGNATOR_OF_FAKE_TESTS"));

		res = bound.saveOrUpdate(res.id, Collections.singletonMap("NM_AIRLINE_DESIGNATOR", "FAKE_DESIGNATOR_OF_FAKE_TESTS2"));

		Assert.assertSame(res.result, Status.updated);

		res = bound.delete(res.id);

		// 6.x doesn't report found
		// Assert.assertEquals(true, res.found);
		Assert.assertSame(res.result, Status.deleted);

		res = bound.delete(res.id);

		Assert.assertSame(res.result, Status.not_found);

		RefreshResult refresh = bound.refresh();

		Assert.assertTrue(refresh.shards.successful > 0);

		BulkDeleteResult bdr = bound.delete(RootQuery.term("NM_AIRLINE_DESIGNATOR", "FAKE_DESIGNATOR_OF_FAKE_TESTS"));

		if (bdr.deleted != 3) {
			log.warn("Delete failed ater refresh ({}). Retrying...", bdr.asJson());
			refresh = bound.refresh();
			Assert.assertTrue(refresh.shards.successful > 0);
			bdr = bound.delete(RootQuery.term("NM_AIRLINE_DESIGNATOR", "FAKE_DESIGNATOR_OF_FAKE_TESTS"));
			Assert.assertEquals(3, bdr.deleted);
		}

	}

	@Before
	public void setup() {
		if (ops.exists("test")) {
			log.info("Deleting index");
			ops.deleteIndex("test");
		}
		log.info("Creating index");

		final IndexDefinition def = IndexDefinition.get().with(Mappings.get().add("doc", IndexedType.get().disableAll().add("NM_AIRLINE_DESIGNATOR", Property.get().with(Type.KEYWORD).with(IndexOptions.docs))));
		ops.createIndex("test", def);
	}
}