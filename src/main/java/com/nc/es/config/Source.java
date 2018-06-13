package com.nc.es.config;

import java.util.HashSet;
import java.util.Set;

public class Source {

	public static Source get() {
		return new Source();
	}

	boolean enabled = true;
	Set<String> includes;
	Set<String> excludes;

	public Source disable() {
		enabled = false;
		return this;
	}

	public Source exclude(String... fields) {
		Set<String> s = excludes;
		if (s == null) {
			s = excludes = new HashSet<>();
		}
		for (final String field : fields) {
			s.add(field);
		}
		return this;
	}

	public Source include(String... fields) {
		Set<String> s = includes;
		if (s == null) {
			s = includes = new HashSet<>();
		}
		for (final String field : fields) {
			s.add(field);
		}
		return this;
	}
}