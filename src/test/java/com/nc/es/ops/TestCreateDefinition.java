package br.atech.commons.es;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;

import org.junit.Assert;
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

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = RestClientConfig.class)
public class TestCreateDefinition {

	@Autowired
	IElasticSearchOps ops;

	@Autowired
	Logger log;

	@Test
	public void run() throws IOException, InterruptedException, BrokenBarrierException {

		if (ops.exists("flow")) {
			Assert.assertTrue(ops.deleteIndex("flow"));
		}

		final Mappings mappings = Mappings.get().add("ovf",
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

}
