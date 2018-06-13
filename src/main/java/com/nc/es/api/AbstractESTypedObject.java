package com.nc.es.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.nc.es.search.ILeafQuery.Term;

public abstract class AbstractESTypedObject<E extends Enum<E>> implements IElasticSearchObject {

	public static <E extends Enum<E>> Term buildTypeTerm(final E value) {
		return Term.term("OBJECT_TYPE", value.toString());
	}

	@JsonProperty("OBJECT_TYPE")
	private final E objectType;

	public AbstractESTypedObject(final E objectType) {
		super();
		this.objectType = objectType;
	}

	public E getObjectType() {
		return objectType;
	}

	public Term getTypeTerm() {
		return buildTypeTerm(objectType);
	}
}
