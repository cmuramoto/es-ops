package com.nc.es.ops;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nc.es.api.IElasticSearchObject;
import com.nc.es.ops.Result.Status;

public interface IBulkUpdateResult extends IElasticSearchObject {

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

	public static class Item {

		@JsonIgnore
		public String id;

		@JsonIgnore
		public int status;

		@JsonIgnore
		public Status result;

		@JsonProperty("update")
		public Update getUpdate() {
			return id == null ? null : new Update(id, status, result);
		}

		public String id() {
			return id;
		}

		@JsonProperty("update")
		public void setUpdate(final Update update) {
			if (update != null) {
				id = update.id;
				status = update.status;
				result = update.result;
			}
		}

		public int status() {
			return status;
		}
	}

	public static class Shallow implements IBulkUpdateResult {
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

	public static class Update {
		public String id;

		public int status;

		public Status result;

		@JsonCreator
		public Update(@JsonProperty("_id") final String id, @JsonProperty("status") final int status, @JsonProperty("result") Status result) {
			this.id = id;
			this.status = status;
			this.result = result;
		}
	}

	boolean errors();

	List<Item> items();

	int took();

}
