package com.nc.util;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public interface JavaTime {

	public static class LocalDateDeserializer extends JsonDeserializer<LocalDate> {

		@Override
		public LocalDate deserialize(JsonParser p, DeserializationContext ctx) throws IOException, JsonProcessingException {
			return LocalDate.parse(p.getText(), ISO_DATE);
		}
	}

	public static class LocalDateSerializer extends JsonSerializer<LocalDate> {
		@Override
		public void serialize(LocalDate t, JsonGenerator g, SerializerProvider p) throws IOException, JsonProcessingException {
			g.writeString(t.format(ISO_DATE));
		}
	}

	public static class LocalDateTimePreciseDeserializer extends JsonDeserializer<LocalDateTime> {

		@Override
		public LocalDateTime deserialize(JsonParser p, DeserializationContext ctx) throws IOException, JsonProcessingException {
			return LocalDateTime.parse(p.getText(), ISO_NO_ZONE_DATE_TIME);
		}
	}

	public static class LocalDateTimePreciseSerializer extends JsonSerializer<LocalDateTime> {
		@Override
		public void serialize(LocalDateTime t, JsonGenerator g, SerializerProvider p) throws IOException, JsonProcessingException {
			g.writeString(t.format(ISO_NO_ZONE_DATE_TIME));
		}
	}

	public static class LocalTimeDeserializer extends JsonDeserializer<LocalTime> {

		@Override
		public LocalTime deserialize(JsonParser p, DeserializationContext ctx) throws IOException, JsonProcessingException {
			return LocalTime.parse(p.getText(), ISO_NO_ZONE_TIME);
		}
	}

	public static class LocalTimeNoMsDeserializer extends JsonDeserializer<LocalTime> {

		@Override
		public LocalTime deserialize(JsonParser p, DeserializationContext ctx) throws IOException, JsonProcessingException {
			return LocalTime.parse(p.getText(), ISO_NO_ZONE_NO_MS_TIME);
		}
	}

	public static class LocalTimeNoMsSerializer extends JsonSerializer<LocalTime> {
		@Override
		public void serialize(LocalTime t, JsonGenerator g, SerializerProvider p) throws IOException, JsonProcessingException {
			g.writeString(t.format(ISO_NO_ZONE_NO_MS_TIME));
		}
	}

	public static class LocalTimeSerializer extends JsonSerializer<LocalTime> {
		@Override
		public void serialize(LocalTime t, JsonGenerator g, SerializerProvider p) throws IOException, JsonProcessingException {
			g.writeString(t.format(ISO_NO_ZONE_TIME));
		}
	}

	String ISO_DATE_PATTERN = "yyyy-MM-dd";
	String ISO_NO_ZONE_NO_MS_TIME_PATTERN = "HH:mm:ss";
	String ISO_NO_ZONE_TIME_PATTERN = "HH:mm:ss.SSS";
	String ISO_NO_ZONE_DATE_TIME_PATTERN = ISO_DATE_PATTERN + "'T'" + ISO_NO_ZONE_TIME_PATTERN;

	DateTimeFormatter ISO_DATE = DateTimeFormatter.ofPattern(ISO_DATE_PATTERN);
	DateTimeFormatter ISO_NO_ZONE_NO_MS_TIME = DateTimeFormatter.ofPattern(ISO_NO_ZONE_NO_MS_TIME_PATTERN);
	DateTimeFormatter ISO_NO_ZONE_TIME = DateTimeFormatter.ofPattern(ISO_NO_ZONE_TIME_PATTERN);
	DateTimeFormatter ISO_NO_ZONE_DATE_TIME = DateTimeFormatter.ofPattern(ISO_NO_ZONE_DATE_TIME_PATTERN);
}