package br.atech.commons.es;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.nc.es.api.IElasticSearchOps;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = RestClientConfig.class)
public class TestVersion {

	@Autowired
	IElasticSearchOps ops;

	@Test
	public void run() {
		System.out.println(ops.info());

		System.out.println(ops.supportsTypes());
	}
}