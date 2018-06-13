package com.nc.es.ops;

import com.nc.es.api.IElasticSearchObject;

public interface ISearchResult extends IElasticSearchObject {

	boolean isEmpty();

	boolean isEmptyOrComplete();

	String scrollId();
}
