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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.hawkular.metrics.core.api.fasterxml.jackson.MetricTypeDeserializer;
import org.hawkular.metrics.core.api.fasterxml.jackson.MetricTypeSerializer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonCreator.Mode;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize.Inclusion;

import io.swagger.annotations.ApiModelProperty;

/**
 * @author jsanda
 */
public class Metric<T> {
    private final MetricId id;
    private final Map<String, String> tags;
    private final Integer dataRetention;
    private final List<DataPoint<T>> dataPoints;

    @JsonCreator(mode = Mode.PROPERTIES)
    @org.codehaus.jackson.annotate.JsonCreator
    public Metric(
            @JsonProperty("id")
            @org.codehaus.jackson.annotate.JsonProperty("id")
            String id,
            @JsonProperty(value = "tags")
            @org.codehaus.jackson.annotate.JsonProperty("tags")
            Map<String, String> tags,
            @JsonProperty("dataRetention")
            @org.codehaus.jackson.annotate.JsonProperty("dataRetention")
            Integer dataRetention,
            @JsonProperty("type")
            @org.codehaus.jackson.annotate.JsonProperty("type")
            @JsonDeserialize(using = MetricTypeDeserializer.class)
            @org.codehaus.jackson.map.annotate.JsonDeserialize(
                    using = org.hawkular.metrics.core.api.codehaus.jackson.MetricTypeDeserializer.class
            )
            MetricType type,
            @JsonProperty("data") @org.codehaus.jackson.annotate.JsonProperty("data")
            List<DataPoint<T>> data
    ) {
        checkArgument(id != null, "Metric id is null");

        type = type == null ? MetricType.UNDEFINED : type;

        this.id = new MetricId("", type, id);
        this.tags = tags == null ? emptyMap() : unmodifiableMap(tags);
        this.dataRetention = dataRetention;
        this.dataPoints = data == null || data.isEmpty() ? emptyList() : unmodifiableList(data);
    }

    public Metric(MetricId id) {
        this(id, Collections.emptyMap(), null, Collections.emptyList());
    }

    public Metric(MetricId id, Map<String, String> tags, Integer dataRetention) {
        this(id, tags, dataRetention, Collections.emptyList());
    }

    public Metric(MetricId id, List<DataPoint<T>> dataPoints) {
        this(id, Collections.emptyMap(), null, dataPoints);
    }

    public Metric(MetricId id, Map<String, String> tags, Integer dataRetention, List<DataPoint<T>> dataPoints) {
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
        this.dataPoints = unmodifiableList(dataPoints);
    }

    public String getId() {
        return getMetricId().getName();
    }

    public String getTenantId() {
        return getMetricId().getTenantId();
    }

    @ApiModelProperty(value = "Metric type", dataType = "string", allowableValues = "gauge, availability, counter")
    @JsonSerialize(using = MetricTypeSerializer.class)
    @org.codehaus.jackson.map.annotate.JsonSerialize(
            using = org.hawkular.metrics.core.api.codehaus.jackson.MetricTypeSerializer.class)
    public MetricType getType() {
        return getMetricId().getType();
    }

    @JsonIgnore
    @org.codehaus.jackson.annotate.JsonIgnore
    public MetricId getMetricId() {
        return id;
    }

    @ApiModelProperty("Metric tags")
    @JsonSerialize(include = Inclusion.NON_EMPTY)
    @org.codehaus.jackson.map.annotate.JsonSerialize(
            include = org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion.NON_EMPTY
    )
    public Map<String, String> getTags() {
        return tags;
    }

    @ApiModelProperty("How long, in days, a data point of this metric stays in the system after it is stored")
    public Integer getDataRetention() {
        return dataRetention;
    }

    @ApiModelProperty("Metric data points")
    @JsonSerialize(include = Inclusion.NON_EMPTY)
    @org.codehaus.jackson.map.annotate.JsonSerialize(
            include = org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion.NON_EMPTY)
    public List<DataPoint<T>> getDataPoints() {
        return dataPoints;
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
                .add("dataPoints", dataPoints)
                .toString();
    }
}
