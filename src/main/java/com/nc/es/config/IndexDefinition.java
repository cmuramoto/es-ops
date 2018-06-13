package com.nc.es.config;

import java.util.Map;

import com.nc.es.api.IElasticSearchObject;

public class IndexDefinition implements IElasticSearchObject {

	public static IndexDefinition get() {
		return new IndexDefinition();
	}

	Settings settings;

	Map<String, IndexedType> mappings;

	@Override
	public String toString() {
		return asPrettyJson();
	}

	public IndexDefinition with(Mappings m) {
		mappings = m.unwrap();
		return this;
	}

	public IndexDefinition with(Settings s) {
		this.settings = s;
		return this;
	}

}