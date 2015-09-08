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
package org.hawkular.metrics.api.jaxrs.model;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

import static org.hawkular.metrics.core.api.MetricType.AVAILABILITY;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.Map;

import org.hawkular.metrics.api.jaxrs.fasterxml.jackson.AvailabilityTypeSerializer;
import org.hawkular.metrics.core.api.AvailabilityType;
import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonCreator.Mode;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize.Inclusion;
import com.google.common.collect.Lists;
import com.wordnik.swagger.annotations.ApiModel;

import rx.Observable;

/**
 * @author John Sanda
 */
@ApiModel(description = "Consists of a timestamp and a value where supported values are \"up\" and \"down\"")
public class AvailabilityDataPoint {
    private final long timestamp;
    private final AvailabilityType value;
    private final Map<String, String> tags;

    @JsonCreator(mode = Mode.PROPERTIES)
    @org.codehaus.jackson.annotate.JsonCreator
    @SuppressWarnings("unused")
    public AvailabilityDataPoint(
            @JsonProperty("timestamp")
            @org.codehaus.jackson.annotate.JsonProperty("timestamp")
            Long timestamp,
            @JsonProperty("value")
            @org.codehaus.jackson.annotate.JsonProperty("value")
            String value,
            @JsonProperty("tags")
            @org.codehaus.jackson.annotate.JsonProperty("tags")
            Map<String, String> tags
    ) {
        checkArgument(timestamp != null, "Data point timestamp is null");
        checkArgument(value != null, "Data point value is null");
        this.timestamp = timestamp;
        this.value = AvailabilityType.fromString(value);
        this.tags = tags == null ? emptyMap() : unmodifiableMap(tags);
    }

    public AvailabilityDataPoint(DataPoint<AvailabilityType> dataPoint) {
        timestamp = dataPoint.getTimestamp();
        value = dataPoint.getValue();
        tags = dataPoint.getTags();
    }

    public long getTimestamp() {
        return timestamp;
    }

    @JsonSerialize(using = AvailabilityTypeSerializer.class)
    @org.codehaus.jackson.map.annotate.JsonSerialize(
            using = org.hawkular.metrics.api.jaxrs.codehaus.jackson.AvailabilityTypeSerializer.class
    )
    public AvailabilityType getValue() {
        return value;
    }

    @JsonSerialize(include = Inclusion.NON_EMPTY)
    @org.codehaus.jackson.map.annotate.JsonSerialize(
            include = org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion.NON_EMPTY
    )
    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AvailabilityDataPoint that = (AvailabilityDataPoint) o;
        return timestamp == that.timestamp && value == that.value;
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return com.google.common.base.Objects.toStringHelper(this)
                .add("timestamp", timestamp)
                .add("value", value)
                .add("tags", tags)
                .omitNullValues()
                .toString();
    }

    public static List<DataPoint<AvailabilityType>> asDataPoints(List<AvailabilityDataPoint> points) {
        return Lists.transform(points, p -> new DataPoint<>(p.getTimestamp(), p.getValue()));
    }

    public static Observable<Metric<AvailabilityType>> toObservable(String tenantId, String
            metricId, List<AvailabilityDataPoint> points) {
        List<DataPoint<AvailabilityType>> dataPoints = asDataPoints(points);
        Metric<AvailabilityType> metric = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, metricId), dataPoints);
        return Observable.just(metric);
    }
}
