package com.nc.es.tuples;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.nc.es.api.IElasticSearchObject;

public final class Tuple2<K, V> extends AbstractMap<K, V> implements IElasticSearchObject, Map.Entry<K, V>, Serializable {

	private static final long serialVersionUID = 1L;

	public static <K, V> Tuple2<K, V> of(final K k, final V v) {
		final Tuple2<K, V> rv = new Tuple2<>(k, v);

		return rv;
	}

	public K k;

	public V v;

	public Tuple2() {
	}

	public Tuple2(final K k, final V v) {
		this.k = k;
		this.v = v;
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return Collections.singleton(this);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		@SuppressWarnings("rawtypes")
		Tuple2 other = (Tuple2) obj;
		if (k == null) {
			if (other.k != null) {
				return false;
			}
		} else if (!k.equals(other.k)) {
			return false;
		}
		if (v == null) {
			if (other.v != null) {
				return false;
			}
		} else if (!v.equals(other.v)) {
			return false;
		}
		return true;
	}

	@Override
	public K getKey() {
		return k;
	}

	@Override
	public V getValue() {
		return v;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = prime;
		result = prime * result + ((k == null) ? 0 : k.hashCode());
		result = prime * result + ((v == null) ? 0 : v.hashCode());
		return result;
	}

	@Override
	public V put(final K key, final V value) {
		final V rv = v;
		k = key;
		v = value;
		return rv;
	}

	@Override
	public V setValue(final V value) {
		final V old = v;
		v = value;
		return old;
	}

	@Override
	public String toString() {
		return asPrettyJson();
	}

}