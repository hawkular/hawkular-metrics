/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.impl;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Duration;
import org.joda.time.Hours;
import org.joda.time.Period;

/**
 * @author John Sanda
 */
public class DateTimeService {

    public Supplier<DateTime> now = DateTime::now;
    public static final Duration DEFAULT_SLICE = Duration.standardDays(1); // Correct place?

    /**
     * @return A DateTime object rounded down to the start of the current hour. For example, if the current time is
     * 17:21:09, then 17:00:00 is returned.
     */
    public DateTime currentHour() {
        return getTimeSlice(now.get(), Hours.ONE.toStandardDuration());
    }

    /**
     * The 24 hour time slices are fix - 00:00 to 24:00. This method determines the 24 hour time slice based on
     * {@link #currentHour()} and returns the start of the time slice.
     *
     * @return A DateTime object rounded down to the start of the current 24 hour time slice.
     */
    public DateTime current24HourTimeSlice() {
        return get24HourTimeSlice(currentHour());
    }

    /**
     * This method determines the 24 hour time slice for the specified time and returns the start of that time slice.
     *
     * @param time The DateTime to be rounded down
     * @return A DateTime rounded down to the start of the 24 hour time slice in which the time parameter falls.
     * @see #current24HourTimeSlice()
     */
    public DateTime get24HourTimeSlice(DateTime time) {
        return getTimeSlice(time, Days.ONE.toStandardDuration());
    }

    public long getTimeSlice(long time, Duration duration) {
        return getTimeSlice(new DateTime(time), duration).getMillis();
    }

    public DateTime getTimeSlice(DateTime dt, Duration duration) {
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

    // DPART calculation methods

    /**
     * Get a matching dpart for a timestamp
     *
     * @param timestamp
     * @param slice
     * @return
     */
    public long getCurrentDpart(long timestamp, Duration slice) {
        DateTime time = new DateTime(timestamp);
        DateTime timeSlice = getTimeSlice(time, slice);
        return timeSlice.getMillis();
    }

    /**
     * Calculate a list of dparts for a given timeslot with given interval
     *
     * @param from
     * @param to
     * @param interval
     * @return
     */
    public long[] getDparts(long from, long to, Duration interval) {
        DateTime start = new DateTime(from);
        DateTime end = new DateTime(to);
        DateTime legacy = new DateTime(0);

        List<DateTime> slices = new LinkedList<>();
        slices.add(legacy);
        DateTime startSlice = getTimeSlice(start, interval);

        while(startSlice.isBefore(end.getMillis())) {
            slices.add(startSlice);
            startSlice = getTimeSlice(startSlice.plus(interval), interval);
        }

        return slices.stream().mapToLong(DateTime::getMillis).toArray();
    }

}
