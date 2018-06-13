package com.nc.es.tuples;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

import com.nc.es.api.IElasticSearchObject;

public final class Tuple4<K, V> extends AbstractMap<String, Object> implements IElasticSearchObject, Serializable {

	private static final long serialVersionUID = 1L;

	public static <B, D> Tuple4<B, D> of(String a, B b, String c, D d) {
		final Tuple4<B, D> rv = new Tuple4<>();
		rv.a = a;
		rv.b = b;
		rv.c = c;
		rv.d = d;

		return rv;
	}

	public String a;
	public K b;
	public String c;
	public V d;

	@Override
	public Set<java.util.Map.Entry<String, Object>> entrySet() {
		final Set<Entry<String, Object>> set = Collections.newSetFromMap(new IdentityHashMap<java.util.Map.Entry<String, Object>, Boolean>(2));
		set.add(Tuple2.of(a, b));
		set.add(Tuple2.of(c, d));

		return set;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		Tuple4 other = (Tuple4) obj;
		if (a == null) {
			if (other.a != null) {
				return false;
			}
		} else if (!a.equals(other.a)) {
			return false;
		}
		if (b == null) {
			if (other.b != null) {
				return false;
			}
		} else if (!b.equals(other.b)) {
			return false;
		}
		if (c == null) {
			if (other.c != null) {
				return false;
			}
		} else if (!c.equals(other.c)) {
			return false;
		}
		if (d == null) {
			if (other.d != null) {
				return false;
			}
		} else if (!d.equals(other.d)) {
			return false;
		}
		return true;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = prime;
		result = prime * result + ((a == null) ? 0 : a.hashCode());
		result = prime * result + ((b == null) ? 0 : b.hashCode());
		result = prime * result + ((c == null) ? 0 : c.hashCode());
		result = prime * result + ((d == null) ? 0 : d.hashCode());
		return result;
	}

	public Tuple2<K, V> vals() {
		return Tuple2.of(b, d);
	}

}
