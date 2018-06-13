package com.nc.es.config;

import java.util.HashMap;
import java.util.Map;

import com.nc.es.api.IElasticSearchObject;

public class Mappings implements IElasticSearchObject {

	public static Mappings get() {
		return new Mappings();
	}

	Map<String, IndexedType> mappings;

	public Mappings add(String name, IndexedType index) {
		Map<String, IndexedType> m = mappings;
		if (m == null) {
			m = mappings = new HashMap<>();
		}
		m.put(name, index);
		return this;
	}

	public Mappings defaultDisableAll() {
		return withDefaults(IndexedType.get().disableAll());
	}

	public IndexedType type(String type) {
		return mappings == null ? null : mappings.get(type);
	}

	Map<String, IndexedType> unwrap() {
		return mappings;
	}

	public Mappings withDefaults(IndexedType type) {
		return add("_default_", type);
	}
}