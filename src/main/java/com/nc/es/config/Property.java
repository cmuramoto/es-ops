package com.nc.es.config;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

public class Property {

	public static enum DateFormat {
		epoch_millis, //
		epoch_second, //
		date_optional_time, //
		strict_date_optional_time, //
		basic_date, //
		basic_date_time, //
		basic_date_time_no_millis, //
		basic_ordinal_date, //
		basic_ordinal_date_time, //
		basic_ordinal_date_time_no_millis, //
		basic_time, //
		basic_time_no_millis, //
		basic_t_time, //
		basic_t_time_no_millis, //
		basic_week_date, //
		strict_basic_week_date, //
		basic_week_date_time, //
		strict_basic_week_date_time, //
		basic_week_date_time_no_millis, //
		strict_basic_week_date_time_no_millis, //
		date, //
		strict_date, //
		date_hour, //
		strict_date_hour, //
		date_hour_minute, //
		strict_date_hour_minute, //
		date_hour_minute_second, //
		strict_date_hour_minute_second, //
		date_hour_minute_second_fraction, //
		strict_date_hour_minute_second_fraction, //
		date_hour_minute_second_millis, //
		strict_date_hour_minute_second_millis, //
		date_time, //
		strict_date_time, //
		date_time_no_millis, //
		strict_date_time_no_millis, //
		hour, //
		strict_hour, //
		hour_minute_second, //
		strict_hour_minute_second, //
		hour_minute_second_fraction, //
		strict_hour_minute_second_fraction, //
		hour_minute_second_millis, //
		strict_hour_minute_second_millis, //
		ordinal_date, //
		strict_ordinal_date, //
		ordinal_date_time, //
		strict_ordinal_date_time, //
		ordinal_date_time_no_millis, //
		strict_ordinal_date_time_no_millis, //
		time, //
		strict_time, //
		time_no_millis, //
		strict_time_no_millis, //
		t_time, //
		strict_t_time, //
		t_time_no_millis, //
		strict_t_time_no_millis, //
		week_date, //
		strict_week_date, //
		week_date_time, //
		strict_week_date_time, //
		week_date_time_no_millis, //
		strict_week_date_time_no_millis, //
		weekyear, //
		strict_weekyear, //
		weekyear_week, //
		strict_weekyear_week, //
		weekyear_week_day, //
		strict_weekyear_week_day, //
		year, //
		strict_year, //
		year_month, //
		strict_year_month, //
		year_month_day, //
		strict_year_month_day;

	}

	public static enum GeneralType {
		BOOLEAN, DATE, DOUBLE, LONG, OBJECT, STRING, ANY;

		@JsonCreator
		public static GeneralType forValue(final String value) {
			return value == null ? null : "*".equals(value) ? ANY : valueOf(value.toUpperCase());
		}

		@JsonValue
		public String toValue() {
			return this == ANY ? "*" : name().toLowerCase();
		}
	}

	public static enum IndexOptions {
		docs, freqs, positions, offsets
	}

	public static enum Type {
		// String
		TEXT, KEYWORD,
		// Numeric
		BYTE, SHORT, INTEGER, FLOAT, LONG, DOUBLE,
		// Date
		DATE,
		// Boolean
		BOOLEAN,
		// Binary
		BINARY,
		// Complex
		OBJECT, NESTED,
		// GEO
		GEO_POINT, GEO_SHAPE,
		// Specialized
		IP, COMPLETION, TOKEN_COUNT,
		// MAPPED_MURMUR3
		// new in 6.x
		JOIN,
		//
		DYNAMIC;

		@JsonCreator
		public static Type forValue(final String value) {
			return value == null ? null : "{dynamic_type}".equals(value) ? DYNAMIC : valueOf(value.toUpperCase());
		}

		@JsonValue
		public String toValue() {
			return this == DYNAMIC ? "{dynamic_type}" : name().toLowerCase();
		}
	}

	public static Property epochMillis() {
		return get().with(Type.DATE).with(DateFormat.epoch_millis);
	}

	public static Property get() {
		return new Property();
	}

	public static Property keyword() {
		final Property rv = get();
		rv.type = Type.KEYWORD;
		return rv;
	}

	Type type;

	DateFormat format;

	Boolean enabled;

	Boolean index;

	Boolean store;

	Boolean dynamic;

	Boolean doc_values;

	@JsonProperty("include_in_all")
	Boolean includeInAll;

	@JsonProperty("index_options")
	IndexOptions options;

	Map<String, Property> fields;

	Map<String, Property> properties;

	String analyzer;

	String search_analyzer;

	public Property add(final String name, final Property prop) {
		Map<String, Property> p = properties;
		if (p == null) {
			p = properties = new HashMap<>();
		}
		p.put(name, prop);

		return this;
	}

	public Property analyzedWith(final String analyzer) {
		this.analyzer = analyzer;

		return this;
	}

	public Property disable() {
		enabled = false;
		return this;
	}

	public Map<String, Property> fields() {
		return fields;
	}

	public Property index() {
		index = true;
		return this;
	}

	public Property noDocValues() {
		doc_values = false;
		return this;
	}

	public Property noIndex() {
		index = false;
		return this;
	}

	public Property noStore() {
		store = false;
		return this;
	}

	public Property omitFromAll() {
		includeInAll = false;
		return this;
	}

	public Property searchedWith(final String analyzer) {
		search_analyzer = analyzer;

		return this;
	}

	public Property stored() {
		store = true;
		return this;
	}

	public Type type() {
		return type;
	}

	public Property with(final DateFormat fmt) {
		format = fmt;
		return this;
	}

	public Property with(final IndexOptions opts) {
		options = opts;
		return this;
	}

	public Property with(final Type type) {
		this.type = type;
		return this;
	}

	public Property withDocValues() {
		doc_values = true;
		return this;
	}

	public Property withSubField(final String name, final Property prop) {
		Map<String, Property> p = fields;
		if (p == null) {
			p = fields = new HashMap<>();
		}
		p.put(name, prop);

		return this;
	}

}