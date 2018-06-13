package com.nc.es.search;

import java.util.HashMap;
import java.util.Map;

import com.nc.es.tuples.Tuple2;

public interface ISpecialQuery extends IQuery {

	public static class Script implements ISpecialQuery {

		public static Script inline(final String script) {
			final Script s = new Script();
			s.source = script;

			return s;
		}

		String source;

		String lang;

		Map<String, Object> params;

		public Script groovy() {
			lang = "groovy";
			return this;
		}

		@Override
		public Kind kind() {
			return Kind.script;
		}

		public Script painless() {
			lang = "painless";
			return this;
		}

		public Script param(final String key, final Object val) {
			Map<String, Object> p = params;
			if (p == null) {
				p = params = new HashMap<>(2);
			}
			p.put(key, val);
			return this;
		}

		@Override
		public Object rewrite() {
			return Tuple2.of(kind(), this);
		}
	}
}