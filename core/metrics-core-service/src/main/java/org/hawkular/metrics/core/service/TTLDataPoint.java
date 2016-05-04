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
package org.hawkular.metrics.core.service;

import java.util.Objects;

import org.hawkular.metrics.model.DataPoint;

import com.google.common.base.MoreObjects;

/**
 * Data points are purged using Cassandra's Time To Live, i.e., TTL, feature. There are scenarios such as tagging
 * data points in which we want to apply the current TTL and not the original TTL. For example, suppose a metric is
 * configured with data retention of one month. A couple tags are applied to a few of the metric's data points when
 * those data points will expire in two weeks. The tags should then expire in two weeks when the corresponding data
 * points expire.
 *
 * @author jsanda
 */
public class TTLDataPoint<T> {

    private final DataPoint<T> dataPoint;

    private final int ttl;

    public TTLDataPoint(DataPoint<T> dataPoint, int ttl) {
        this.dataPoint = dataPoint;
        this.ttl = ttl;
    }

    /**
     * The {@link DataPoint data point}
     */
    public DataPoint<T> getDataPoint() {
        return dataPoint;
    }

    /**
     * The current TTL or remaining time before the data point expires. Note that this is not exact as the actual
     * TTL is a function of time and therefore constantly changing.
     */
    public int getTTL() {
        return ttl;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TTLDataPoint<?> that = (TTLDataPoint<?>) o;
        return Objects.equals(ttl, that.ttl) &&
                Objects.equals(dataPoint, that.dataPoint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataPoint, ttl);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("dataPoint", dataPoint)
                .add("ttl", ttl)
                .toString();
    }
}
