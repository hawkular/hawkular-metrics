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

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author jsanda
 */
public class Metric<T> {
    private final MetricId<T> id;
    private final Map<String, String> tags;
    private final Integer dataRetention;
    private final Integer bucketSize;
    private final List<DataPoint<T>> dataPoints;

    public Metric(MetricId<T> id) {
        this(id, Collections.emptyMap(), null, null, Collections.emptyList());
    }

    public Metric(MetricId<T> id, Map<String, String> tags, Integer dataRetention, Integer bucketSize) {
        this(id, tags, dataRetention, bucketSize, Collections.emptyList());
    }

    public Metric(MetricId<T> id, List<DataPoint<T>> dataPoints) {
        this(id, Collections.emptyMap(), null,
                null, dataPoints);
    }

    public Metric(MetricId<T> id, Map<String, String> tags, Integer dataRetention, Integer bucketSize,
                  List<DataPoint<T>>
            dataPoints) {
        checkArgument(id != null, "id is null");
        checkArgument(tags != null, "tags is null");
        checkArgument(dataPoints != null, "dataPoints is null");
        this.id = id;
        this.tags = unmodifiableMap(tags);
        // If the data_retention column is not set, the driver returns zero instead of null.
        // We are (at least for now) using null to indicate that the metric does not have
        // the data retention set.
        if (dataRetention == null || dataRetention == 0) {
            this.dataRetention = null;
        } else {
            this.dataRetention = dataRetention;
        }
        if (bucketSize != null && bucketSize != 0) {
            this.bucketSize = bucketSize;
        } else {
            this.bucketSize = null;
        }

        this.dataPoints = unmodifiableList(dataPoints);
    }

    @Deprecated
    public String getTenantId() {
        return getId().getTenantId();
    }

    @Deprecated
    public MetricType<T> getType() {
        return getId().getType();
    }

    public MetricId<T> getId() {
        return id;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public Integer getDataRetention() {
        return dataRetention;
    }

    public List<DataPoint<T>> getDataPoints() {
        return dataPoints;
    }

    public Integer getBucketSize() {
        return bucketSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Metric<?> metric = (Metric<?>) o;
        return Objects.equals(id, metric.id) &&
                Objects.equals(tags, metric.tags) &&
                Objects.equals(dataRetention, metric.dataRetention);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, tags, dataRetention);
    }

    @Override
    public String toString() {
        return com.google.common.base.Objects.toStringHelper(this)
                .add("id", id)
                .add("tags", tags)
                .add("dataRetention", dataRetention)
                .toString();
    }
}
