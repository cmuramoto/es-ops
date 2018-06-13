package br.atech.commons.es;

import com.nc.es.api.IElasticSearchObject;

public class ESObject implements IElasticSearchObject {

	@Override
	public String toString() {
		return asPrettyJson();
	}

}
