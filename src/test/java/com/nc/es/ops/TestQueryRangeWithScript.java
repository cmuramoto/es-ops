package com.nc.es.ops;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.nc.es.api.IElasticSearchOps;
import com.nc.es.api.IElasticSearchOps.IBound;
import com.nc.es.config.DynamicTemplate;
import com.nc.es.config.IndexDefinition;
import com.nc.es.config.IndexedType;
import com.nc.es.config.Mappings;
import com.nc.es.search.TimeSpan;
import com.nc.es.search.ICompoundQuery.Bool;
import com.nc.es.search.ILeafQuery.Range;
import com.nc.es.search.ISpecialQuery.Script;
import com.nc.es.tuples.Tuple2;
import com.nc.util.JavaTime;

/**
 * Displays script functionality for querying documents by a date range (e.g. between 2017/01/01 and
 * 2017/01/31) and then by hours within the days (e.g. between 08:00 to 13:00). <br>
 * This use case is better off handled by employing dates and times in two separate fields. In the
 * database we are used to employ the 'trunc' function to discard times and other functions to
 * extract times, however such functionality in ElasticSearch is not natively present in the API.
 * <br>
 * To work around this we can use scripted queries to extract specific values from docs in order to
 * perform the queries!
 *
 * @author cmuramoto
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = RestClientConfig.class)
public class TestQueryRangeWithScript {

	static class VO {
		int id;

		LocalDateTime timestamp;
	}

	@Autowired
	IElasticSearchOps ops;

	@Test
	public void run() {
		final IBound bound = ops.bind("test", "vo");

		VO vo = new VO();
		vo.id = 0;
		vo.timestamp = LocalDateTime.of(2017, 1, 1, 8, 0);
		bound.insert(vo);

		vo = new VO();
		vo.id = 1;
		vo.timestamp = LocalDateTime.of(2017, 1, 1, 9, 0);
		bound.insert(vo);

		vo = new VO();
		vo.id = 2;
		vo.timestamp = LocalDateTime.of(2017, 1, 1, 10, 20);
		bound.insert(vo);

		vo = new VO();
		vo.id = 3;
		vo.timestamp = LocalDateTime.of(2017, 1, 31, 10, 10);
		bound.insert(vo);

		vo = new VO();
		vo.id = 4;
		vo.timestamp = LocalDateTime.of(2017, 2, 1, 10, 0);
		bound.insert(vo);

		bound.refresh();

		testScriptInline(bound);

		testTimespan(bound);

	}

	@Before
	public void setup() {
		ops.deleteIfExists("test");

		ops.createIndex("test", IndexDefinition.get().with(Mappings.get().withDefaults(IndexedType.get().autoDetectingDatesWith(JavaTime.ISO_NO_ZONE_DATE_TIME_PATTERN).with("xxx", DynamicTemplate.keywordDefaults()))));
	}

	private void testScriptInline(IBound bound) {
		LoggerFactory.getLogger(getClass()).info("Testing inline-script");
		// Between [01/01,31/01]
		// We must supply a pattern otherwise it will use the full pattern associated to the field
		// (yyyy-MM-dd'T'HH:mm:ss.SSS) and parsing will fail
		final Range dates = Range.range("timestamp").isoDate().gte(LocalDate.of(2017, 1, 1)).lte(LocalDate.of(2017, 1, 31));
		// Between [08:00,10:20]
		Script times = Script.inline("doc.timestamp.date.getHourOfDay() >= params.minH && doc.timestamp.date.getHourOfDay() <= params.maxH && doc.timestamp.date.getMinuteOfHour() >= params.minM && doc.timestamp.date.getMinuteOfHour() <= params.maxM").param("minH", 8).param("maxH", 10).param("minM", 0).param("maxM", 20);
		Bool q = Bool.create().filter(dates, times);

		{
			final Set<Integer> matches = bound.query(VO.class, q.asRoot()).vals().map(v -> v.id).collect(Collectors.toSet());
			Assert.assertEquals(4, matches.size());
			Assert.assertTrue(matches.contains(0));
			Assert.assertTrue(matches.contains(1));
			Assert.assertTrue(matches.contains(2));
			Assert.assertTrue(matches.contains(3));
		}

		// Between [08:00,10:19]
		times = Script.inline("doc.timestamp.date.getHourOfDay() >= params.minH && doc.timestamp.date.getHourOfDay() <= params.maxH && doc.timestamp.date.getMinuteOfHour() >= params.minM && doc.timestamp.date.getMinuteOfHour() <= params.maxM").param("minH", 8).param("maxH", 10).param("minM", 0).param("maxM", 19);
		q = Bool.create().filter(dates, times);

		{
			final Set<Integer> matches = bound.query(VO.class, q.asRoot()).vals().map(v -> v.id).collect(Collectors.toSet());
			Assert.assertEquals(3, matches.size());
			Assert.assertTrue(matches.contains(0));
			Assert.assertTrue(matches.contains(1));
			Assert.assertTrue(matches.contains(3));
		}
	}

	private void testTimespan(IBound bound) {

		Range.range("timestamp").isoDate().gte(LocalDate.of(2017, 1, 1)).lte(LocalDate.of(2017, 1, 31));
		final TimeSpan ts = TimeSpan.get().startingAt(2017, 1, 1).endingAt(2017, 1, 31).beginTime(8, 0).endTime(10, 20);
		LoggerFactory.getLogger(getClass()).info("Testing timestamp {}", ts.asJson());
		Tuple2<Range, Script> range = ts.toRange("timestamp");

		Bool q = Bool.create().filter(range.k, range.v);
		{
			final Set<Integer> matches = bound.query(VO.class, q.asRoot()).vals().map(v -> v.id).collect(Collectors.toSet());
			Assert.assertEquals(4, matches.size());
			Assert.assertTrue(matches.contains(0));
			Assert.assertTrue(matches.contains(1));
			Assert.assertTrue(matches.contains(2));
			Assert.assertTrue(matches.contains(3));
		}

		// Between [08:00,10:19]
		range = TimeSpan.get().startingAt(2017, 1, 1).endingAt(2017, 1, 31).beginTime(8, 0).endTime(10, 19).toRange("timestamp");
		q = Bool.create().filter(range.k, range.v);

		{
			final Set<Integer> matches = bound.query(VO.class, q.asRoot()).vals().map(v -> v.id).collect(Collectors.toSet());
			Assert.assertEquals(3, matches.size());
			Assert.assertTrue(matches.contains(0));
			Assert.assertTrue(matches.contains(1));
			Assert.assertTrue(matches.contains(3));
		}
	}

}