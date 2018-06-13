package com.nc.es.search;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import com.nc.es.api.IElasticSearchObject;
import com.nc.es.search.ILeafQuery.Range;
import com.nc.es.search.ISpecialQuery.Script;
import com.nc.es.tuples.Tuple2;

public class TimeSpan implements IElasticSearchObject {

	public static TimeSpan get() {
		return new TimeSpan();
	}

	LocalDate beginDay;

	LocalDate endDay;

	LocalTime beginTime;

	LocalTime endTime;

	public int beginMinuteOffset() {
		LocalTime lt = beginTime;
		return lt == null ? -1 : 60 * lt.getHour() + lt.getMinute();
	}

	public TimeSpan beginTime(int offsetMinutes) {
		return beginTime(offsetMinutes / 60, offsetMinutes % 60);
	}

	public TimeSpan beginTime(int h, int m) {
		beginTime = LocalTime.of(h, m);
		return this;
	}

	public TimeSpan beginTime(LocalTime time) {
		beginTime = time;
		return this;
	}

	public int daySpan() {
		return (int) (endDay == null ? 1 : endDay.toEpochDay() - beginDay.toEpochDay());
	}

	public TimeSpan endingAt(int year, int month, int day) {
		endDay = LocalDate.of(year, month, day);
		return this;
	}

	public TimeSpan endingAt(LocalDate end) {
		endDay = end;
		return this;
	}

	public int endMinuteOffset() {
		LocalTime lt = endTime;
		return lt == null ? -1 : 60 * lt.getHour() + lt.getMinute();
	}

	public TimeSpan endTime(int offsetMinutes) {
		return endTime(offsetMinutes / 60, offsetMinutes % 60);
	}

	public TimeSpan endTime(int h, int m) {
		endTime = LocalTime.of(h, m);
		return this;
	}

	public TimeSpan endTime(LocalTime time) {
		endTime = time;
		return this;
	}

	public LocalDate getBeginDay() {
		return beginDay;
	}

	public LocalTime getBeginTime() {
		return beginTime;
	}

	public LocalDate getEndDay() {
		return endDay;
	}

	public LocalTime getEndTime() {
		return endTime;
	}

	public TimeSpan startingAt(int year, int month, int day) {
		beginDay = LocalDate.of(year, month, day);
		return this;
	}

	public TimeSpan startingAt(LocalDate begin) {
		beginDay = begin;
		return this;
	}

	public Duration timeDuration() {
		return Duration.between(beginTime, endTime);
	}

	public Range toExactDate(String term) {
		Range r;

		if (beginDay != null) {
			r = Range.range(term).isoDate().exact(beginDay);
		} else {
			r = null;
		}

		return r;
	}

	public Range toExactDateTime(String term) {

		Range r;

		if (beginDay != null) {
			if (beginTime != null) {
				r = Range.range(term).isoDateTime().exact(LocalDateTime.of(beginDay, beginTime));
			} else {
				r = toExactDate(term);
			}
		} else {
			r = null;
		}

		return r;

	}

	public Tuple2<Range, Script> toRange(String term) {
		Range r;
		Script s;

		if (beginDay != null) {
			r = Range.range(term).isoDate().gte(beginDay);
			if (endDay != null) {
				r = r.lte(endDay);
			}
		} else if (endDay != null) {
			r = Range.range(term).isoDate().lte(endDay);
		} else {
			r = null;
		}

		if (beginTime != null) {
			if (endTime != null) {
				final String txt = String.format("doc.%1$s.date.getHourOfDay() >= params.minH && doc.%1$s.date.getHourOfDay() <= params.maxH && doc.%1$s.date.getMinuteOfHour() >= params.minM && doc.%1$s.date.getMinuteOfHour() <= params.maxM", term);
				s = Script.inline(txt).param("minH", beginTime.getHour()).param("maxH", endTime.getHour()).param("minM", beginTime.getMinute()).param("maxM", endTime.getMinute());
			} else {
				final String txt = String.format("doc.%1$s.date.getHourOfDay() >= params.minH && doc.%1$s.date.getMinuteOfHour() >= params.minM", term);
				s = Script.inline(txt).param("minH", beginTime.getHour()).param("minM", beginTime.getMinute());
			}
		}
		if (endTime != null) {
			final String txt = String.format("doc.%1$s.date.getHourOfDay() <= params.maxH && doc.%1$s.date.getMinuteOfHour() <= params.maxM", term);
			s = Script.inline(txt).param("maxH", endTime.getHour()).param("maxM", endTime.getMinute());
		} else {
			s = null;
		}

		return r == null && s == null ? null : Tuple2.of(r, s);
	}

}
