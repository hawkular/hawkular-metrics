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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

/**
 * A numeric metric data point. This class currently represents both raw and aggregated data; however, at some point
 * we may subclasses for each of them.
 *
 * @author John Sanda
 */
public class NumericData extends MetricData {

    private double value;

    private Set<AggregatedValue> aggregatedValues = new HashSet<>();

    @JsonCreator
    public NumericData(@JsonProperty("timestamp") long timestamp, @JsonProperty("value") double value) {
        super(timestamp);
        this.value = value;
    }

    public NumericData(UUID timeUUID, double value) {
        super(timeUUID);
        this.value = value;
    }

    public NumericData(UUID timeUUID, double value, Map<String, String> tags) {
        super(timeUUID, tags);
        this.value = value;
    }

    public NumericData(UUID timeUUID, double value, Map<String, String> tags, Long writeTime) {
        super(timeUUID, tags, writeTime);
        this.value = value;
    }

    /**
     * The value of the raw data point. This should only be set for raw data. It should be null for aggregated data.
     */
    public double getValue() {
        return value;
    }

    public NumericData setValue(double value) {
        this.value = value;
        return this;
    }

    /**
     * A set of the aggregated values that make up this aggregated data point. This should return an empty set for raw
     * data.
     */
    public Set<AggregatedValue> getAggregatedValues() {
        return aggregatedValues;
    }

    public NumericData addAggregatedValue(AggregatedValue aggregatedValue) {
        aggregatedValues.add(aggregatedValue);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NumericData)) return false;
        if (!super.equals(o)) return false;

        NumericData that = (NumericData) o;

        if (Double.compare(that.value, value) != 0) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        long temp;
        temp = Double.doubleToLongBits(value);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("timeUUID", timeUUID)
            .add("timestamp", getTimestamp())
            .add("value", value)
            .toString();
    }
}
