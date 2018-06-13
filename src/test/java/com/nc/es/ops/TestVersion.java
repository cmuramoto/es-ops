package br.atech.commons.es;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.nc.es.api.IElasticSearchOps;
import com.nc.es.api.NodeInfo;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = RestClientConfig.class)
public class TestVersion {

	@Autowired
	IElasticSearchOps ops;

	@Test
	public void run() {
		Logger log = LoggerFactory.getLogger(getClass());
		NodeInfo info = ops.info();
		log.info("{}", info);

		boolean st = ops.supportsTypes();
		log.info("{}", st);
	}
}