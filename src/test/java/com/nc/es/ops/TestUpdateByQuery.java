package br.atech.commons.es;

import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.nc.es.api.IElasticSearchOps;
import com.nc.es.api.IElasticSearchOps.IBound;
import com.nc.es.config.IndexDefinition;
import com.nc.es.search.RootQuery;
import com.nc.es.search.ILeafQuery.Exists;
import com.nc.es.search.ILeafQuery.Term;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = RestClientConfig.class)
public class TestUpdateByQuery {

	static class VO {
		int value;

		String tag;

		public VO() {
		}

		public VO(final int value) {
			super();
			this.value = value;
			tag = "foo" + value;
		}

		String id() {
			return Integer.toString(value);
		}
	}

	@Autowired
	IElasticSearchOps ops;

	@Test
	public void run() {
		final IBound bound = ops.bind("test");

		Assert.assertTrue(bound.exists());

		Assert.assertFalse(bound.bulkInsert(IntStream.range(0, 100).mapToObj(VO::new), VO::id, 100).errors());

		bound.refresh();

		Assert.assertEquals(100, bound.count(Exists.exists("tag").asRoot()));

		final RootQuery even = com.nc.es.search.ISpecialQuery.Script.inline("(doc['value'].value & 1)==0").asRoot();

		Assert.assertEquals(50, bound.count(even));

		bound.updateByQuery(even.updating("ctx._source.tag='even'"));

		bound.refresh();

		Assert.assertEquals(50, bound.count(Term.term("tag", "even").asRoot()));

		Assert.assertEquals(50, bound.deleteFields(even, "tag").updated);

		bound.refresh();

		Assert.assertEquals(50, bound.count(Exists.exists("tag").asRoot()));
	}

	@Before
	public void setup() {
		final String index = "test";
		ops.deleteIfExists(index);

		ops.createIndex(index, IndexDefinition.get());
	}

	@After
	public void tearDown() {
		ops.deleteIfExists("test");
	}

}
