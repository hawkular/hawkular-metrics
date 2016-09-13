/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.scheduler.api;

import static org.hawkular.metrics.datetime.DateTimeService.currentMinute;
import static org.hawkular.metrics.datetime.DateTimeService.getTimeSlice;
import static org.hawkular.metrics.datetime.DateTimeService.now;
import static org.joda.time.Duration.standardMinutes;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.joda.time.Minutes;

/**
 * @author jsanda
 */
public class RepeatingTrigger implements Trigger {

    private Long triggerTime;

    private Long interval;

    private Long delay;

    private Integer repeatCount;

    private Integer executionCount;

    public static class Builder {

        private Long triggerTime;
        private Long interval = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);
        private Long delay;
        private Integer repeatCount;

        public Builder withTriggerTime(long time) {
            this.triggerTime = time;
            return this;
        }

        public Builder withInterval(int interval, TimeUnit timeUnit) {
            this.interval = TimeUnit.MILLISECONDS.convert(interval, timeUnit);
            return this;
        }

        public Builder withDelay(int delay, TimeUnit timeUnit) {
            this.delay = TimeUnit.MILLISECONDS.convert(delay, timeUnit);
            return this;
        }

        public Builder withRepeatCount(int count) {
            this.repeatCount = count;
            return this;
        }

        public RepeatingTrigger build() {
            return new RepeatingTrigger(triggerTime, interval, delay, repeatCount);
        }

    }

    private RepeatingTrigger() {
    }

    private RepeatingTrigger(Long triggerTime, Long interval, Long delay, Integer repeatCount) {
        if (triggerTime != null) {
            this.triggerTime = getTimeSlice(triggerTime, standardMinutes(1));
        }
        else if (interval == null && delay == null) {
            this.triggerTime = currentMinute().plusMinutes(1).getMillis();
        }

        this.interval = interval;
        this.delay = delay == null ? Minutes.ONE.toStandardDuration().getMillis() : delay;
        this.repeatCount = repeatCount;
        this.executionCount = 1;

        if (this.triggerTime == null) {
            this.triggerTime = getTimeSlice(now.get().getMillis() + this.delay, standardMinutes(1));
        }
    }

    // TODO reduce visibility?
    // This is for internal use by TaskSchedulerImpl.
    public RepeatingTrigger(long interval, long delay, long triggerTime, int repeatCount, int executionCount) {
        this.interval = interval;
        this.delay = delay;
        this.triggerTime = triggerTime;
        this.executionCount = executionCount;
        this.repeatCount = repeatCount == 0 ? null : repeatCount;
        this.executionCount = executionCount;
    }

    public long getInterval() {
        return interval;
    }

    public long getDelay() {
        return delay;
    }

    @Override
    public long getTriggerTime() {
        return triggerTime;
    }

    public Integer getRepeatCount() {
        return repeatCount;
    }

    public int getExecutionCount() {
        return executionCount;
    }

    @Override
    public Trigger nextTrigger() {
        // TODO what should we do if interval is null?

        if (repeatCount != null && executionCount + 1 > repeatCount) {
            return null;
        }
        RepeatingTrigger next = new RepeatingTrigger();
        next.interval = interval;
        next.delay = delay;
        next.triggerTime = triggerTime + interval;
        next.repeatCount = repeatCount;
        next.executionCount = executionCount + 1;

        return next;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RepeatingTrigger that = (RepeatingTrigger) o;
        return Objects.equals(triggerTime, that.triggerTime) &&
                Objects.equals(interval, that.interval) &&
                Objects.equals(delay, that.delay);
    }

    @Override
    public int hashCode() {
        return Objects.hash(triggerTime, interval, delay);
    }

    @Override
    public String toString() {
        return "RepeatingTrigger{" +
                "triggerTime=" + triggerTime +
                ", interval=" + interval +
                ", delay=" + delay +
                '}';
    }

}
