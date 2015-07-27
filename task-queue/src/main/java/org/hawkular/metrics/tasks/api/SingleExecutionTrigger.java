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
package org.hawkular.metrics.tasks.api;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;

/**
 * @author jsanda
 */
public class SingleExecutionTrigger implements Trigger {

    private long triggerTime;

    public static class Builder {

        private long delay;

        private Long triggerTime;

        public Builder withDelay(long delay, TimeUnit timeUnit) {
            this.delay = TimeUnit.MILLISECONDS.convert(delay, timeUnit);
            return this;
        }

        public Builder withTriggerTime(long time) {
            this.triggerTime = time;
            return this;
        }

        public SingleExecutionTrigger build() {
            SingleExecutionTrigger trigger = new SingleExecutionTrigger();
            if (triggerTime == null) {
                trigger.triggerTime = getExecutionDateTime(System.currentTimeMillis()).getMillis();
            }
            trigger.triggerTime += delay;
            return trigger;
        }
    }

    private SingleExecutionTrigger() {
    }

    public SingleExecutionTrigger(long triggerTime) {
        this.triggerTime = getExecutionDateTime(triggerTime).getMillis();
    }

    @Override
    public long getTriggerTime() {
        return triggerTime;
    }

    @Override
    public Trigger nextTrigger() {
        return null;
    }

    private static DateTime getExecutionDateTime(long time) {
        DateTime dt = new DateTime(time);
        Duration duration = Duration.millis(1000);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SingleExecutionTrigger that = (SingleExecutionTrigger) o;
        return Objects.equals(triggerTime, that.triggerTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(triggerTime);
    }

    @Override
    public String toString() {
        return "SingleExecutionTrigger{" +
                "triggerTime=" + triggerTime +
                '}';
    }
}
