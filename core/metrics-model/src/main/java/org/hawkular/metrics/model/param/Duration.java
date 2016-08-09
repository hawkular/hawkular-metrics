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
package org.hawkular.metrics.model.param;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableBiMap;

/**
 * A time duration. This class is meant to be used only as a JAX−RS method parameter. If you need to work with a
 * duration, prefer usage of {@link org.joda.time.Duration}.
 *
 * @author Thomas Segismont
 */
public class Duration {
    public static final ImmutableBiMap<TimeUnit, String> UNITS = new ImmutableBiMap.Builder<TimeUnit, String>()
            .put(MILLISECONDS, "ms")
            .put(SECONDS, "s")
            .put(MINUTES, "mn")
            .put(HOURS, "h")
            .put(DAYS, "d")
            .build();

    private static final Pattern REGEXP = Pattern.compile(
            "(\\d+)"
                    + "("
                    + Duration.UNITS.values().stream().collect(joining("|"))
                    + ")"
    );

    private static final ImmutableBiMap<String, TimeUnit> STRING_UNITS = Duration.UNITS.inverse();

    private final long value;
    private final TimeUnit timeUnit;

    /**
     * Create a new instance. The list of valid time units is the following:
     * <ul>
     *     <li><em>ms</em>: milliseconds</li>
     *     <li><em>s</em>: seconds</li>
     *     <li><em>mn</em>: minutes</li>
     *     <li><em>h</em>: hours</li>
     *     <li><em>d</em>: days</li>
     * </ul>
     *
     * @param value    amount of time
     * @param timeUnit time unit, cannot be null, must be one of the valid units
     */
    public Duration(long value, TimeUnit timeUnit) {
        checkArgument(timeUnit != null, "timeUnit is null");
        checkArgument(UNITS.containsKey(timeUnit), "Invalid unit %s", timeUnit);
        this.value = value;
        this.timeUnit = timeUnit;
    }

    public Duration(String duration) {
        Matcher matcher = REGEXP.matcher(duration);
        checkArgument(matcher.matches(), "Invalid duration %s", duration);

        long value=Long.valueOf(matcher.group(1));
        TimeUnit timeUnit=STRING_UNITS.get(matcher.group(2));

        checkArgument(timeUnit != null, "timeUnit is null");
        checkArgument(UNITS.containsKey(timeUnit), "Invalid unit %s", timeUnit);
        this.value = value;
        this.timeUnit = timeUnit;
    }

    public long getValue() {
        return value;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public long toMillis() {
        return MILLISECONDS.convert(value, timeUnit);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Duration duration = (Duration) o;
        return value == duration.value && timeUnit == duration.timeUnit;

    }

    @Override
    public int hashCode() {
        int result = (int) (value ^ (value >>> 32));
        result = 31 * result + timeUnit.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Duration[" + "value=" + value + ", timeUnit=" + timeUnit + ']';
    }
}
