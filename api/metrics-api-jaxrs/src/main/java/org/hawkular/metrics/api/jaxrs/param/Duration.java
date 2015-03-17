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
package org.hawkular.metrics.api.jaxrs.param;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.concurrent.TimeUnit;

/**
 * A time duration. This class is meant to be used only as a JAX−RS method parameter. If you need to work with a
 * duration, prefer usage of {@link org.joda.time.Duration}.
 *
 * @author Thomas Segismont
 * @see DurationConverter
 */
public class Duration {
    private final long value;
    private final TimeUnit timeUnit;

    /**
     * @param value    amount of time
     * @param timeUnit time unit, cannot be null
     */
    public Duration(long value, TimeUnit timeUnit) {
        checkArgument(timeUnit != null, "timeUnit is null");
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
