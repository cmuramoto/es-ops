package br.atech.commons.es;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.StopWatch;

import com.nc.es.api.IElasticSearchOps;
import com.nc.es.api.IElasticSearchOps.IBound;
import com.nc.es.config.IndexDefinition;
import com.nc.es.config.IndexedType;
import com.nc.es.config.Mappings;
import com.nc.es.config.Property;
import com.nc.es.config.Settings;
import com.nc.es.config.Source;
import com.nc.es.config.Property.Type;
import com.nc.es.ops.BulkDeleteResult;
import com.nc.es.ops.IBulkInsertResult;
import com.nc.es.search.RootQuery;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = RestClientConfig.class)
public class TestBulkInsert {

	static final AtomicInteger q = new AtomicInteger();

	// NR_FLIGHTINTENTION_ID;NR_PARENT_ID;NR_CALC_EET;NR_EOBT;NR_CALC_ETA;NR_ETA;NR_FLIGHTINTENTION_TYPE;NR_FLIGHTPLAN_ACTIVATION;NM_FLIGHTPLAN_INDICATIVE;NM_START_INDICATIVE;NM_END_INDICATIVE;NR_CRUISE_LEVEL;NM_EQUIPMENT;NR_REGULATED_ID;NR_REGULATED_TYPE;NM_REGULATED;NM_REGULATED_FAKE;NR_INST;NR_DT_INST;NR_SESSION_ID;NR_DT_SESSION_BEGIN;NM_AIRLINE_DESIGNATOR;NR_FLIGHTPLAN_STATE;NM_EQUIPMENT_CATEGORY;NM_EQUIPMENT_WAKE_TURBULENCE
	static final String[] FIELDS = { "NR_FLIGHTINTENTION_ID", "NR_PARENT_ID", "NR_CALC_EET", "NR_EOBT", "NR_CALC_ETA", "NR_ETA", "NR_FLIGHTINTENTION_TYPE", "NR_FLIGHTPLAN_ACTIVATION", "NM_FLIGHTPLAN_INDICATIVE", "NM_START_INDICATIVE", "NM_END_INDICATIVE", "NR_CRUISE_LEVEL", "NM_EQUIPMENT", "NR_REGULATED_ID", "NR_REGULATED_TYPE", "NM_REGULATED", "NM_REGULATED_FAKE", "NR_INST", "NR_DT_INST",
			"NR_SESSION_ID", "NR_DT_SESSION_BEGIN", "NM_AIRLINE_DESIGNATOR", "NR_FLIGHTPLAN_STATE", "NM_EQUIPMENT_CATEGORY", "NM_EQUIPMENT_WAKE_TURBULENCE" };

	static void doValue(StringBuilder sb, String s, int ix) {
		switch (ix) {
		case 8:
		case 9:
		case 10:
		case 12:
		case 15:
		case 16:
		case 21:
		case 23:
		case 24:
			sb.append('"').append(s).append('"');
			break;
		default:
			sb.append(s);
			break;
		}
	}

	static String mapForStore(BufferedReader br, int max) throws IOException {
		String l;

		final StringBuilder sb = new StringBuilder();

		for (int i = 0; i < max && (l = br.readLine()) != null; i++) {
			sb.append("{ \"index\": {}}\n");
			sb.append("{");
			final String[] vals = l.split(";");

			for (int j = 0; j < FIELDS.length && j < vals.length; j++) {
				final String s = vals[j];
				if (s != null && !s.isEmpty()) {
					sb.append('"').append(FIELDS[j]).append('"').append(":");
					doValue(sb, s, j);
					sb.append(',');
				}
			}
			sb.delete(sb.length() - 1, sb.length());
			sb.append("}\n");
		}

		return sb.toString();
	}

	@Autowired
	IElasticSearchOps ops;

	@Autowired
	Logger log;

	@Before
	public void createIndexDefinition() throws IOException, InterruptedException, BrokenBarrierException {

		if (ops.exists("flow")) {
			Assert.assertTrue(ops.deleteIndex("flow"));
		}

		final Mappings mappings = Mappings.get().add("doc",
				IndexedType.get().disableAll().add("NM_AIRLINE_DESIGNATOR", Property.keyword()) //
						.add("NM_END_INDICATIVE", Property.keyword()).add("NM_EQUIPMENT", Property.keyword()).add("NM_EQUIPMENT_CATEGORY", Property.keyword()).add("NM_EQUIPMENT_WAKE_TURBULENCE", Property.keyword())//
						.add("NM_FLIGHTPLAN_INDICATIVE", Property.keyword()).add("NM_REGULATED", Property.keyword()).add("NM_REGULATED_FAKE", Property.keyword()).add("NM_START_INDICATIVE", Property.keyword())//
						.add("NR_CALC_EET", Property.get().with(Type.LONG)).add("NR_CALC_ETA", Property.epochMillis()).add("NR_CRUISE_LEVEL", Property.get().with(Type.DOUBLE)).add("NR_DT_INST", Property.epochMillis())//
						.add("NR_DT_SESSION_BEGIN", Property.epochMillis()).add("NR_EOBT", Property.epochMillis()).add("NR_ETA", Property.epochMillis()).add("NR_FLIGHTINTENTION_ID", Property.get().with(Type.LONG))//
						.add("NR_FLIGHTINTENTION_TYPE", Property.get().with(Type.INTEGER)).add("NR_FLIGHTPLAN_ACTIVATION", Property.get().with(Type.INTEGER)).add("NR_FLIGHTPLAN_STATE", Property.get().with(Type.INTEGER))//
						.add("NR_INST", Property.get().with(Type.INTEGER)).add("NR_PARENT_ID", Property.get().with(Type.LONG)).add("NR_REGULATED_ID", Property.get().with(Type.LONG))//
						.add("NR_REGULATED_TYPE", Property.get().with(Type.INTEGER)).add("NR_SESSION_ID", Property.get().with(Type.INTEGER)).with(Source.get().exclude("NM_EQUIPMENT_WAKE_TURBULENCE"))//
		);

		final IndexDefinition def = IndexDefinition.get().with(Settings.get().withReplicas(1).withShards(16)).with(mappings);
		final String definition = def.asPrettyJson();

		log.info("Index Definition:\n{}", definition);

		Assert.assertTrue(ops.createIndex("flow", def));

		log.info("Done");
	}

	private String map(String l) {
		final StringBuilder sb = new StringBuilder();

		final String[] vals = l.split(";");

		sb.append("{");
		for (int j = 0; j < FIELDS.length && j < vals.length; j++) {
			final String s = vals[j];
			if (s != null && !s.isEmpty()) {
				sb.append('"').append(FIELDS[j]).append('"').append(":");
				doValue(sb, s, j);
				sb.append(',');
			}
		}
		sb.setCharAt(sb.length() - 1, '}');

		return sb.toString();
	}

	@Test
	public void run() throws IOException {
		final IBound bound = ops.bind("flow", "ovf");

		final StopWatch sw = new StopWatch();
		sw.start("Mini");
		try (BufferedReader br = Files.newBufferedReader(Paths.get("src/test/resources/overfly_top30K.csv"), Charset.defaultCharset())) {
			br.readLine();

			final Set<String> ids = bound.bulkInsertRaw(IBulkInsertResult.Deep.class, br.lines().map(this::map), 997).flatMap(s -> s.items().stream().map(i -> i.id())).collect(Collectors.toSet());

			Assert.assertEquals(30_000, ids.size());
		}
		sw.stop();

		bound.refresh();

		final BulkDeleteResult result = bound.delete(RootQuery.matchAll());
		Assert.assertEquals(30_000, result.deleted);

		sw.start("Huge");
		try (BufferedReader br = Files.newBufferedReader(Paths.get("src/test/resources/overfly_top30K.csv"), Charset.defaultCharset())) {
			br.readLine();

			final Set<String> ids = bound.bulkInsertRaw(IBulkInsertResult.Deep.class, br.lines().map(this::map), 100_000_000).flatMap(s -> s.items().stream().map(i -> i.id())).collect(Collectors.toSet());

			Assert.assertEquals(30_000, ids.size());
		}
		sw.stop();

		log.info(sw.prettyPrint());
	}
}