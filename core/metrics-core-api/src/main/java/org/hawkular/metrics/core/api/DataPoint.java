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
package org.hawkular.metrics.core.api;

import java.util.Comparator;
import java.util.Objects;

/**
 * A metric data point consists of a timestamp and a value. The data type of the value will vary depending on the metric
 * type. The data point may also include tags which are stored as a map of key/value pairs.
 *
 * @author jsanda
 */
public class DataPoint<T> implements Comparable<T> {

    public static final Comparator<DataPoint<?>> TIMESTAMP_COMPARATOR = Comparator.comparing(DataPoint::getTimestamp);

    private final long timestamp;

    private final T value;

    public DataPoint(long timestamp, T value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public T getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataPoint<?> dataPoint = (DataPoint<?>) o;
        return Objects.equals(timestamp, dataPoint.timestamp) &&
                Objects.equals(value, dataPoint.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, value);
    }

    @Override
    public String toString() {
        return com.google.common.base.Objects.toStringHelper(this)
                .add("timestamp", timestamp)
                .add("value", value)
                .toString();
    }

    @Override
    public int compareTo(T t) {
        return ((DataPoint) t).getTimestamp() > getTimestamp() ? 1 : 0;
    }
}
