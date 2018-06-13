package br.atech.commons.es;

import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;

import com.nc.es.api.IElasticSearchOps;
import com.nc.es.imp.ElasticSearchOps;

public class RestClientConfig {

	@Bean
	Logger log() {
		return LoggerFactory.getLogger(getClass());
	}

	@Bean
	@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
	IElasticSearchOps ops() {
		return new ElasticSearchOps(new HttpHost("localhost", 9200), 30000);
	}

}
