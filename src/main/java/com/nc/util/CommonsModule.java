package com.nc.util;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import com.fasterxml.jackson.databind.module.SimpleModule;

public class CommonsModule extends SimpleModule {

	private static final long serialVersionUID = 1L;

	public CommonsModule() {
		addSerializer(LocalDateTime.class, new JavaTime.LocalDateTimePreciseSerializer()).addDeserializer(LocalDateTime.class, new JavaTime.LocalDateTimePreciseDeserializer());
		addSerializer(LocalDate.class, new JavaTime.LocalDateSerializer()).addDeserializer(LocalDate.class, new JavaTime.LocalDateDeserializer());
		addSerializer(LocalTime.class, new JavaTime.LocalTimeNoMsSerializer()).addDeserializer(LocalTime.class, new JavaTime.LocalTimeNoMsDeserializer());
	}

}
