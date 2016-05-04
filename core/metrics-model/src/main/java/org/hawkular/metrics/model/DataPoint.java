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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import io.swagger.annotations.ApiModelProperty;

/**
 * A metric data point consists of a timestamp and a value. The data type of the value will vary depending on the metric
 * type. The data point may also include tags which are stored as a map of key/value pairs.
 *
 * @author jsanda
 */
public class DataPoint<T> {

    protected final long timestamp;

    protected final T value;

    protected final Map<String, String> tags;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public DataPoint(
            @JsonProperty("timestamp")
            Long timestamp,
            @JsonProperty("value")
            T value
    ) {
        checkArgument(timestamp != null, "Data point timestamp is null");
        checkArgument(value != null, "Data point value is null");
        this.timestamp = timestamp;
        this.value = value;
        tags = Collections.emptyMap();
    }

    public DataPoint(Long timestamp, T value, Map<String, String> tags) {
        checkArgument(timestamp != null, "Data point timestamp is null");
        checkArgument(value != null, "Data point value is null");
        this.timestamp = timestamp;
        this.value = value;
        this.tags = Collections.unmodifiableMap(tags);
    }

    @ApiModelProperty(required = true)
    public long getTimestamp() {
        return timestamp;
    }

    @ApiModelProperty(required = true)
    public T getValue() {
        return value;
    }

    @ApiModelProperty(required = false)
    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataPoint<?> dataPoint = (DataPoint<?>) o;
        return Objects.equals(timestamp, dataPoint.timestamp) &&
                Objects.equals(value, dataPoint.value) &&
                Objects.equals(tags, dataPoint.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, value, tags);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("timestamp", timestamp)
                .add("value", value)
                .add("tags", tags)
                .omitNullValues()
                .toString();
    }
}
