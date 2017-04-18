/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkular.metrics.datetime;

import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjuster;
import java.util.function.Supplier;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.Duration;
import org.joda.time.Hours;
import org.joda.time.Period;

/**
 * @author John Sanda
 */
public class DateTimeService {

    static {
        DateTimeZone.setDefault(DateTimeZone.UTC);
    }

    public static Supplier<DateTime> now = DateTime::now;

    /**
     * @return A DateTime object rounded down to the start of the current minute. For example if the current time is
     * 17:21:09, then 17:21:00 is returned.
     */
    public static DateTime currentMinute() {
        return getTimeSlice(now.get(), Duration.standardMinutes(1));
    }

    /**
     * @return A DateTime object rounded down to the start of the current hour. For example, if the current time is
     * 17:21:09, then 17:00:00 is returned.
     */
    public static DateTime currentHour() {
        return getTimeSlice(now.get(), Hours.ONE.toStandardDuration());
    }

    /**
     * The 24 hour time slices are fix - 00:00 to 24:00. This method determines the 24 hour time slice based on
     * {@link #currentHour()} and returns the start of the time slice.
     *
     * @return A DateTime object rounded down to the start of the current 24 hour time slice.
     */
    public static DateTime current24HourTimeSlice() {
        return get24HourTimeSlice(currentHour());
    }

    /**
     * This method determines the 24 hour time slice for the specified time and returns the start of that time slice.
     *
     * @param time The DateTime to be rounded down
     * @return A DateTime rounded down to the start of the 24 hour time slice in which the time parameter falls.
     * @see #current24HourTimeSlice()
     */
    public static DateTime get24HourTimeSlice(DateTime time) {
        return getTimeSlice(time, Days.ONE.toStandardDuration());
    }

    public static long getTimeSlice(long time, Duration duration) {
        return getTimeSlice(new DateTime(time), duration).getMillis();
    }

    public static DateTime getTimeSlice(DateTime dt, Duration duration) {
        Period p = duration.toPeriod();

        if (p.getYears() != 0) {
            return dt.yearOfEra().roundFloorCopy().minusYears(dt.getYearOfEra() % p.getYears());
        } else if (p.getMonths() != 0) {
            return dt.monthOfYear().roundFloorCopy().minusMonths((dt.getMonthOfYear() - 1) % p.getMonths());
        } else if (p.getWeeks() != 0) {
            return dt.weekOfWeekyear().roundFloorCopy().minusWeeks((dt.getWeekOfWeekyear() - 1) % p.getWeeks());
        } else if (p.getDays() != 0) {
            return dt.dayOfMonth().roundFloorCopy().minusDays((dt.getDayOfMonth() - 1) % p.getDays());
        } else if (p.getHours() != 0) {
            return dt.hourOfDay().roundFloorCopy().minusHours(dt.getHourOfDay() % p.getHours());
        } else if (p.getMinutes() != 0) {
            return dt.minuteOfHour().roundFloorCopy().minusMinutes(dt.getMinuteOfHour() % p.getMinutes());
        } else if (p.getSeconds() != 0) {
            return dt.secondOfMinute().roundFloorCopy().minusSeconds(dt.getSecondOfMinute() % p.getSeconds());
        }
        return dt.millisOfSecond().roundCeilingCopy().minusMillis(dt.getMillisOfSecond() % p.getMillis());
    }

    public static TemporalAdjuster startOfNextOddHour() {
        return temporal -> {
            int currentHour = temporal.get(ChronoField.HOUR_OF_DAY);
            return temporal.plus((currentHour % 2 == 0) ? 1 : 2, ChronoUnit.HOURS)
                    .with(ChronoField.MINUTE_OF_HOUR, 0)
                    .with(ChronoField.SECOND_OF_MINUTE, 0)
                    .with(ChronoField.NANO_OF_SECOND, 0);
        };
    }

    public static TemporalAdjuster startOfPreviousEvenHour() {
        return temporal -> {
            int currentHour = temporal.get(ChronoField.HOUR_OF_DAY);
            return temporal.minus((currentHour % 2 == 0) ? 0 : 1, ChronoUnit.HOURS)
                    .with(ChronoField.MINUTE_OF_HOUR, 0)
                    .with(ChronoField.SECOND_OF_MINUTE, 0)
                    .with(ChronoField.NANO_OF_SECOND, 0);
        };
    }

}
