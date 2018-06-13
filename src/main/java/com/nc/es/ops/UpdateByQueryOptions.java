package com.nc.es.ops;

import java.lang.reflect.Field;

public class UpdateByQueryOptions {

	String conflicts = "proceed";

	public Boolean pretty;

	public Boolean refresh;

	public Boolean wait_for_completion;

	public Boolean wait_for_active_shards;

	public Integer timeout;

	public Integer slices;

	private StringBuilder append(StringBuilder sb, final String name, final Object v) {
		if (v != null) {
			if (sb == null) {
				sb = new StringBuilder();
			}
			if (sb.length() > 0) {
				sb.append('&');
			}

			sb.append(name).append('=').append(v);
		}

		return sb;
	}

	public UpdateByQueryOptions async() {
		wait_for_completion = false;
		return this;
	}

	public UpdateByQueryOptions disableConcurrentUpdates() {
		conflicts = null;
		return this;
	}

	public String toQueryString() {

		final Field[] fields = getClass().getDeclaredFields();
		StringBuilder sb = null;

		try {
			for (final Field field : fields) {
				sb = append(sb, field.getName(), field.get(this));
			}
		} catch (final Throwable e) {
			e.printStackTrace();
		}

		return sb == null ? null : sb.toString();
	}
}
