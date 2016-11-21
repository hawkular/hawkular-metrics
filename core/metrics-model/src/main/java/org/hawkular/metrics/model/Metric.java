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
package org.hawkular.metrics.model;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.hawkular.metrics.model.fasterxml.jackson.MetricTypeDeserializer;
import org.hawkular.metrics.model.fasterxml.jackson.MetricTypeSerializer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonCreator.Mode;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize.Inclusion;
import com.google.common.base.MoreObjects;

import io.swagger.annotations.ApiModelProperty;

/**
 * @author jsanda
 */
public class Metric<T> {
    private final MetricId<T> id;
    private final Map<String, String> tags;
    private final Integer dataRetention;
    private final List<DataPoint<T>> dataPoints;
    private final Long minTimestamp;
    private final Long maxTimestamp;

    @SuppressWarnings("unchecked")
    @JsonCreator(mode = Mode.PROPERTIES)
    public Metric(
            @JsonProperty("id")
            String id,
            @JsonProperty(value = "tags")
            Map<String, String> tags,
            @JsonProperty("dataRetention")
            Integer dataRetention,
            @JsonProperty("type")
            @JsonDeserialize(using = MetricTypeDeserializer.class)
            MetricType<T> type,
            @JsonProperty("data")
            List<DataPoint<T>> data,
            @JsonProperty(value="tenantId", defaultValue="")
            String tenantId
    ) {
        checkArgument(id != null, "Metric id is null");

        if (type == null) {
            type = MetricType.UNDEFINED;
        }

        if (tenantId == null) {
            tenantId = "";
        }

        this.id = new MetricId<T>(tenantId, type, id);
        this.tags = tags == null ? emptyMap() : unmodifiableMap(tags);
        this.dataRetention = dataRetention;
        this.dataPoints = data == null || data.isEmpty() ? emptyList() : unmodifiableList(data);
        this.minTimestamp = this.maxTimestamp = null;
    }

    public Metric(String id, Map<String, String> tags, Integer dataRetention, MetricType<T> type,
            List<DataPoint<T>> data) {
        this(id, tags, dataRetention, type, data, "");
    }

    public Metric(MetricId<T> id) {
        this(id, Collections.emptyMap(), null, Collections.emptyList());
    }

    public Metric(MetricId<T> id, Map<String, String> tags) {
        this(id, unmodifiableMap(tags), null, Collections.emptyList());
    }

    public Metric(MetricId<T> id, Integer dataRetention) {
        this(id, Collections.emptyMap(), dataRetention, Collections.emptyList());
    }

    public Metric(MetricId<T> id, Map<String, String> tags, Integer dataRetention) {
        this(id, tags, dataRetention, Collections.emptyList());
    }

    public Metric(MetricId<T> id, List<DataPoint<T>> dataPoints) {
        this(id, Collections.emptyMap(), null, dataPoints);
    }

    public Metric(MetricId<T> id, List<DataPoint<T>> dataPoints, Integer dataRetention) {
        this(id, Collections.emptyMap(), dataRetention, dataPoints);
    }

    public Metric(MetricId<T> id, Map<String, String> tags, Integer dataRetention, List<DataPoint<T>> dataPoints) {
        checkArgument(id != null, "id is null");
        checkArgument(tags != null, "tags is null");
        checkArgument(dataPoints != null, "dataPoints is null");
        this.id = id;
        this.tags = unmodifiableMap(tags);
        this.dataRetention = dataRetention;
        this.dataPoints = unmodifiableList(dataPoints);
        this.minTimestamp = this.maxTimestamp = null;
    }

    /**
     * Creates a new instance by copying the specified one, except the min and max timestamp fields.
     *
     * @param other        the instance to copy
     * @param minTimestamp the new value for min timestamp
     * @param maxTimestamp the new value for max timestamp
     */
    public Metric(Metric<T> other, long minTimestamp, long maxTimestamp) {
        this.id = other.id;
        this.tags = other.tags;
        this.dataRetention = other.dataRetention;
        this.dataPoints = other.dataPoints;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
    }

    public String getId() {
        return getMetricId().getName();
    }

    public String getTenantId() {
        return getMetricId().getTenantId();
    }

    @ApiModelProperty(value = "Metric type", dataType = "string", allowableValues = "gauge, availability, counter")
    @JsonSerialize(using = MetricTypeSerializer.class)
    public MetricType<T> getType() {
        return getMetricId().getType();
    }

    @JsonIgnore
    public MetricId<T> getMetricId() {
        return id;
    }

    @ApiModelProperty("Metric tags")
    @JsonSerialize(include = Inclusion.NON_EMPTY)
    public Map<String, String> getTags() {
        return tags;
    }

    @ApiModelProperty("How long, in days, a data point of this metric stays in the system after it is stored")
    public Integer getDataRetention() {
        return dataRetention;
    }

    @ApiModelProperty("Metric data points")
    @JsonSerialize(include = Inclusion.NON_EMPTY)
    public List<DataPoint<T>> getDataPoints() {
        return dataPoints;
    }

    @ApiModelProperty("Timestamp of the metric's oldest data point")
    @JsonSerialize(include = Inclusion.NON_EMPTY)
    public Long getMinTimestamp() {
        return minTimestamp;
    }

    @ApiModelProperty("Timestamp of the metric's most recent data point")
    @JsonSerialize(include = Inclusion.NON_EMPTY)
    public Long getMaxTimestamp() {
        return maxTimestamp;
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
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("tags", tags)
                .add("dataRetention", dataRetention)
                .add("dataPoints", dataPoints)
                .toString();
    }
}
