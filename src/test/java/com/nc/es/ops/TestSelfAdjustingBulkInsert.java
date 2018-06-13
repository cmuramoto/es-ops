package com.nc.es.ops;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.nc.es.api.IElasticSearchOps;
import com.nc.es.config.IndexDefinition;
import com.nc.es.config.IndexedType;
import com.nc.es.config.Mappings;
import com.nc.es.config.Property;
import com.nc.es.config.Settings;
import com.nc.es.config.Property.Type;
import com.nc.es.rest.client.Response;
import com.nc.es.rest.client.RestClient;
import com.nc.es.rest.sniff.Sniffer;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = RestClientConfig.class)
public class TestSelfAdjustingBulkInsert {

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
						.add("NR_REGULATED_TYPE", Property.get().with(Type.INTEGER)).add("NR_SESSION_ID", Property.get().with(Type.INTEGER))//
		);

		final IndexDefinition def = IndexDefinition.get().with(Settings.get().withReplicas(1).withShards(16)).with(mappings);
		final String definition = def.asPrettyJson();

		log.info("Index Definition:\n{}", definition);

		Assert.assertTrue(ops.createIndex("flow", def));

		log.info("Done");
	}

	@Test
	public void run() throws IOException {
		try (RestClient client = RestClient.builder(new HttpHost("localhost", 9200)).build(); Sniffer sniffer = Sniffer.builder(client).build()) {
			try (BufferedReader br = Files.newBufferedReader(Paths.get("src/test/resources/overfly_top30K.csv"), Charset.defaultCharset())) {
				br.readLine();
				int batch = 500;
				int minBatch = 1000, maxBatch = Integer.MIN_VALUE;
				double minElapsed = Double.MAX_VALUE;
				double maxElapsed = Double.MIN_VALUE;
				double avgElapsed = 0;

				int loop = 0;
				while (true) {
					final String json = mapForStore(br, batch);

					if (!json.isEmpty()) {
						HttpEntity e = new NStringEntity(json, ContentType.APPLICATION_JSON);

						final long now = System.currentTimeMillis();
						final Response response = client.performRequest("POST", "/flow/ovf/_bulk", Collections.<String, String> emptyMap(), e);
						if (response.getStatusLine().getStatusCode() != 200) {
							log.error("{}", response);
						} else {
							e = response.getEntity();
							try (InputStream is = e.getContent()) {
								final byte[] chunk = new byte[is.available()];
								is.read(chunk, 0, chunk.length);
								final String s = new String(chunk);
								log.info(s);
							}
						}
						final double elapsed = System.currentTimeMillis() - now;
						final double normElapsed = elapsed / batch;

						avgElapsed = avgElapsed + (normElapsed - avgElapsed) / (loop + 1);

						if (normElapsed < avgElapsed) {
							batch += 100;
						} else {
							batch -= 100;
						}

						if (elapsed > maxElapsed) {
							maxElapsed = elapsed;
						}

						if (elapsed < minElapsed) {
							minElapsed = elapsed;
						}

						if (batch < minBatch) {
							minBatch = batch;
						}

						if (batch > maxBatch) {
							maxBatch = batch;
						}

						log.info(String.format("Loop: %d. Batch: %d. MinBatch: %d. MaxBatch: %d. MinElapsed: %.4f. MaxElapsed: %.4f. AvgElapsed: %.4f", loop++, batch, minBatch, maxBatch, minElapsed, maxElapsed, avgElapsed));
					} else {
						break;
					}

				}
			}

		}
	}

}