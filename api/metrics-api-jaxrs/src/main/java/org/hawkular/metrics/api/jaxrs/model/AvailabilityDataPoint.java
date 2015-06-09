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

import java.util.Map;
import java.util.Objects;

import org.hawkular.metrics.core.api.AvailabilityType;
import org.hawkular.metrics.core.api.DataPoint;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.wordnik.swagger.annotations.ApiModel;

/**
 * @author jsanda
 */
@ApiModel(description = "Consists of a timestamp and a value where supported values are \"up\" and \"down\"")
public class AvailabilityDataPoint {

    @JsonProperty
    private long timestamp;

    @JsonProperty
    private String value;

    @JsonProperty
    private Map<String, String> tags = emptyMap();

    /**
     * Used by JAX-RS/Jackson to deserialize HTTP request data
     */
    private AvailabilityDataPoint() {
    }

    /**
     * Used to prepared data for serialization into the HTTP response
     *
     * @param dataPoint
     */
    public AvailabilityDataPoint(DataPoint<AvailabilityType> dataPoint) {
        timestamp = dataPoint.getTimestamp();
        value = dataPoint.getValue().getText().toLowerCase();
        tags = dataPoint.getTags();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getValue() {
        return value.toLowerCase();
    }

    public Map<String, String> getTags() {
        return ImmutableMap.copyOf(tags);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AvailabilityDataPoint dataPoint = (AvailabilityDataPoint) o;
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
                .add("tags", tags)
                .toString();
    }
}
