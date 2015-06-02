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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author jsanda
 */
public class AvailabilityDataPoint implements DataPoint<AvailabilityType> {

    private final long timestamp;

    @JsonProperty("value")
    private final AvailabilityType value;

    private Map<String, String> tags = Collections.emptyMap();

    @JsonCreator
    public AvailabilityDataPoint(@JsonProperty("timestamp") long timestamp,
            @JsonProperty("value") AvailabilityType value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    public AvailabilityDataPoint(long timestamp, String value) {
        this.timestamp = timestamp;
        this.value = AvailabilityType.fromString(value);
    }

    public AvailabilityDataPoint(long timestamp, AvailabilityType value, Map<String, String> tags) {
        this.timestamp = timestamp;
        this.value = value;
        this.tags = tags;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public AvailabilityType getValue() {
        return value;
    }

    @Override
    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AvailabilityDataPoint that = (AvailabilityDataPoint) o;
        return Objects.equals(timestamp, that.timestamp) &&
                Objects.equals(value, that.value);
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
