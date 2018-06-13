package br.atech.commons.es;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.nc.es.api.IElasticSearchOps;
import com.nc.es.api.IElasticSearchOps.IBound;
import com.nc.es.config.DynamicTemplate;
import com.nc.es.config.IndexDefinition;
import com.nc.es.config.IndexedType;
import com.nc.es.config.Mappings;
import com.nc.es.config.Property;
import com.nc.es.config.Property.GeneralType;
import com.nc.es.config.Property.Type;
import com.nc.es.ops.SimpleSearchResult;
import com.nc.es.search.RootQuery;
import com.nc.es.search.agg.DateHistogram;
import com.nc.es.search.agg.Terms;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = RestClientConfig.class)
public class TestAggregation {

	static class VO {

		String user;

		LocalDateTime timestamp;
	}

	@Autowired
	IElasticSearchOps ops;

	IBound bound;

	private void check(List<Map<String, Object>> subList, int ix, int count) {
		final String key = "user#" + ix;
		final Map<String, Object> map = subList.stream().filter(m -> key.equals(m.get("key"))).findFirst().orElseThrow(() -> new IllegalStateException());
		final int val = ((Number) map.get("doc_count")).intValue();

		Assert.assertEquals(count, val);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void run() {
		final LocalDateTime t0 = LocalDateTime.of(2017, 02, 01, 10, 15, 0), t1 = LocalDateTime.of(2017, 02, 01, 10, 15, 1), t2 = LocalDateTime.of(2017, 02, 01, 10, 15, 2);

		// [0:1,1:1,2:1]
		for (int i = 0; i < 3; i++) {
			final VO vo = new VO();
			vo.timestamp = t0;
			vo.user = "user#" + i;

			bound.insert(vo);
		}

		// [0:4,1:3,2:3]
		for (int i = 0; i < 10; i++) {
			final VO vo = new VO();
			vo.timestamp = t1;
			vo.user = "user#" + i % 3;

			bound.insert(vo);
		}

		// [0:50,1:50]
		for (int i = 0; i < 100; i++) {
			final VO vo = new VO();
			vo.timestamp = t2;
			vo.user = "user#" + i % 2;
			bound.insert(vo);
		}

		bound.refresh();

		final SimpleSearchResult query = bound.query(RootQuery.matchAll().aggregate("AA", DateHistogram.on("timestamp").withInterval(1, TimeUnit.SECONDS).nest("YY", Terms.on("user").asc().upTo(5))).limit(0));

		final Map<String, Object> aggs = query.aggregations;

		final Map<String, Object> object = (Map<String, Object>) aggs.get("AA");

		final List<Map<String, Object>> list = (List<Map<String, Object>>) object.get("buckets");

		for (final Map<String, Object> map : list) {
			final String key = (String) map.get("key_as_string");
			final Number dc = (Number) map.get("doc_count");
			final Map<String, Object> subs = (Map<String, Object>) map.get("YY");
			final List<Map<String, Object>> subList = (List<Map<String, Object>>) subs.get("buckets");

			switch (key) {
			case "2017-02-01T10:15:00.000":
				Assert.assertEquals(3, dc.intValue());
				check(subList, 0, 1);
				check(subList, 1, 1);
				check(subList, 1, 1);

				break;
			case "2017-02-01T10:15:01.000":
				Assert.assertEquals(10, dc.intValue());
				check(subList, 0, 4);
				check(subList, 1, 3);
				check(subList, 2, 3);
				break;
			case "2017-02-01T10:15:02.000":
				Assert.assertEquals(100, dc.intValue());
				check(subList, 0, 50);
				check(subList, 1, 50);
				break;
			}

		}

	}

	@Before
	public void setup() {
		final IndexDefinition def = IndexDefinition.get().with(Mappings.get().withDefaults(IndexedType.get().disableAll().autoDetectingDatesWith("yyyy/MM/dd", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS").with("only_keywords", DynamicTemplate.of(GeneralType.STRING).mappingTo(Property.get().with(Type.KEYWORD)))));
		final String boolIx = "bool_ix";

		ops.deleteIfExists(boolIx);

		ops.createIndex(boolIx, def);

		bound = ops.bind(boolIx, "vo");
		bound.delete(RootQuery.matchAll());
	}
}