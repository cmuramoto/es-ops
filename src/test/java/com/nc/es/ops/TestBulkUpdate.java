package com.nc.es.ops;

import java.util.List;
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
import com.nc.es.ops.IBulkUpdateResult;
import com.nc.es.ops.Result;
import com.nc.es.ops.UpdateStatement;
import com.nc.es.ops.IBulkUpdateResult.Deep;
import com.nc.es.ops.IBulkUpdateResult.Item;
import com.nc.es.ops.MappedSearchResult.Hit;
import com.nc.es.ops.Result.Status;
import com.nc.es.ops.UpdateStatement.Header;
import com.nc.es.ops.UpdateStatement.Script;
import com.nc.es.ops.UpdateStatement.ScriptWithOptions;
import com.nc.es.search.RootQuery;
import com.nc.es.tuples.Tuple2;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = RestClientConfig.class)
public class TestBulkUpdate {

	static class VO {
		int value;

		public VO() {
		}

		public VO(final int value) {
			super();
			this.value = value;
		}

		String id() {
			return Integer.toString(value);
		}
	}

	static Tuple2<String, VO> fromHit(final Hit<VO> h) {
		return Tuple2.of(h.id, h.source);
	}

	static UpdateStatement maskScript(final Tuple2<String, Integer> val) {
		final Header h = new Header();
		h.retries = 5;
		final ScriptWithOptions s = new ScriptWithOptions(h);
		// s.lang = "painless";
		final int value = val.v.intValue();

		final int shift = value / 10 + (value % 10 == 0 ? 0 : 1);

		s.source = "ctx._source.value = 1 << params.shift";
		s.setParameter("shift", shift);

		return s;
	}

	static UpdateStatement script(final Tuple2<String, VO> val) {
		final Script s = new Script();
		// s.lang = "painless";

		if ((Integer.parseInt(val.k) & 1) == 0) {
			s.source = "ctx._source.value *= params.mul";
			s.setParameter("mul", 4);
		} else {
			s.source = "ctx._source.value += 1";
		}

		return s;
	}

	static Tuple2<String, VO> toUpgratedTuple(final int value) {
		final String id = Integer.toString(value);
		return Tuple2.of(id, new VO(value * 3));
	}

	@Autowired
	IElasticSearchOps ops;

	@Test
	public void run() {
		final IBound bound = ops.bind("test");

		Assert.assertFalse(bound.bulkInsert(IntStream.range(0, 100).mapToObj(VO::new), VO::id, 100).errors());

		bound.refresh();

		Assert.assertFalse(bound.bulkUpdate(IntStream.range(0, 100).mapToObj(TestBulkUpdate::toUpgratedTuple), 100).errors());

		bound.refresh();

		List<Hit<VO>> hits = bound.query(VO.class, RootQuery.matchAll().limit(1000)).hits();

		Assert.assertEquals(100, hits.size());

		for (final Hit<VO> hit : hits) {
			Assert.assertEquals(Integer.parseInt(hit.id) * 3, hit.source.value);
		}

		bound.refresh();

		Assert.assertFalse(bound.bulkUpdate(hits.stream().map(TestBulkUpdate::fromHit), TestBulkUpdate::script, 100).errors());

		bound.refresh();

		hits = bound.query(VO.class, RootQuery.matchAll().limit(1000)).hits();

		Assert.assertEquals(100, hits.size());

		for (final Hit<VO> hit : hits) {
			final int id = Integer.parseInt(hit.id);
			final int value = hit.source.value;

			if ((id & 1) == 0) {
				Assert.assertEquals(id * 3 * 4, value);
			} else {
				Assert.assertEquals(id * 3 + 1, value);
			}
		}

		Deep deep = bound.bulkUpdate(IBulkUpdateResult.Deep.class, IntStream.range(0, 100).mapToObj(v -> Tuple2.of(String.valueOf(v), Tuple2.of("value", 0))), 100).reduce(IBulkUpdateResult.Deep::merge).orElse(null);

		List<Item> items = deep.items();

		// Zero has never beeen updated change
		Assert.assertEquals(99, items.stream().filter(i -> i.result == Status.updated).count());

		Assert.assertSame(Result.Status.noop, items.stream().filter(item -> "0".equals(item.id)).findFirst().orElseThrow(() -> new AssertionError()).result);

		deep = bound.bulkUpdate(IBulkUpdateResult.Deep.class, IntStream.range(0, 100).mapToObj(v -> Tuple2.of(String.valueOf(v), v)), TestBulkUpdate::maskScript, 100).reduce(IBulkUpdateResult.Deep::merge).orElse(null);
		items = deep.items();

		Assert.assertEquals(100, items.stream().filter(i -> i.result == Status.updated).count());

		bound.refresh();

		hits = bound.query(VO.class, RootQuery.matchAll().limit(1000)).hits();

		Assert.assertEquals(100, hits.size());

		for (final Hit<VO> hit : hits) {
			final int v = Integer.parseInt(hit.id);
			final int shift = v / 10 + (v % 10 == 0 ? 0 : 1);
			Assert.assertEquals(1 << shift, hit.source.value);
		}
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
