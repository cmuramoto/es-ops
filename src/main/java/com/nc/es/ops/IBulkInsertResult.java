package com.nc.es.ops;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nc.es.api.IElasticSearchObject;

public interface IBulkInsertResult extends IElasticSearchObject {

	public static class Deep extends Shallow {
		List<Item> items;

		@Override
		public List<Item> items() {
			return items;
		}

		public Deep merge(final Deep d) {
			super.merge(d);

			if (items != null) {
				if (d.items != null) {
					items.addAll(d.items);
				}
			} else if (d.items != null) {
				items = d.items;
			}

			return this;
		}
	}

	public static class Index {
		String id;

		int status;

		@JsonCreator
		public Index(@JsonProperty("_id") final String id, @JsonProperty("status") final int status) {
			this.id = id;
			this.status = status;
		}
	}

	public static class Item {

		@JsonIgnore
		String id;

		@JsonIgnore
		int status;

		@JsonProperty("index")
		public Index getIndex() {
			return id == null ? null : new Index(id, status);
		}

		public String id() {
			return id;
		}

		@JsonProperty("index")
		public void setIndex(final Index index) {
			if (index != null) {
				id = index.id;
				status = index.status;
			}
		}

		public int status() {
			return status;
		}
	}

	public static class Shallow implements IBulkInsertResult {
		int took;

		boolean errors;

		@Override
		public boolean errors() {
			return errors;
		}

		@Override
		public List<Item> items() {
			return Collections.emptyList();
		}

		public Shallow merge(final Shallow s) {
			errors &= s.errors;
			took += s.took;

			return this;
		}

		@Override
		public int took() {
			return took;
		}

		@Override
		public String toString() {
			return asPrettyJson();
		}
	}

	boolean errors();

	List<Item> items();

	int took();
}
