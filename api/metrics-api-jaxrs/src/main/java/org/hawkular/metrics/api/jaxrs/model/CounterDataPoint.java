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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.wordnik.swagger.annotations.ApiModel;
import org.hawkular.metrics.core.api.DataPoint;

/**
 * @author jsanda
 */
@ApiModel(description = "A timestamp and a value where the value is interpreted as a signed 64 bit integer")
public class CounterDataPoint {

    @JsonProperty
    private long timestamp;

    @JsonProperty
    private Long value;

    @JsonProperty
    private Map<String, String> tags = Collections.emptyMap();

    /**
     * Used by JAX-RS/Jackson to deserialize HTTP request data
     */
    private CounterDataPoint() {
    }

    /**
     * Used to prepared data for serialization into the HTTP response
     *
     * @param dataPoint
     */
    public CounterDataPoint(DataPoint<Long> dataPoint) {
        timestamp = dataPoint.getTimestamp();
        value = dataPoint.getValue();
        tags = dataPoint.getTags();
    }

    public Long getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Map<String, String> getTags() {
        return ImmutableMap.copyOf(tags);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CounterDataPoint that = (CounterDataPoint) o;
        // TODO should tags be included in equals?
        return Objects.equals(timestamp, that.timestamp) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        // TODO should tags be included?
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
