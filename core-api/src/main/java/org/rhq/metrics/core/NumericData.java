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
package org.rhq.metrics.core;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Objects;

import org.rhq.metrics.util.TimeUUIDUtils;

/**
 * A numeric metric data point. This class currently represents both raw and aggregated data; however, at some point
 * we may subclasses for each of them.
 *
 * @author John Sanda
 */
public class NumericData extends MetricData {

    private double value;

    private Set<AggregatedValue> aggregatedValues = new HashSet<>();

    public NumericData(NumericMetric metric, long timestamp, double value) {
        this(metric, TimeUUIDUtils.getTimeUUID(timestamp), value);
    }

    public NumericData(NumericMetric metric, UUID timeUUID, double value) {
        super(metric, timeUUID);
        this.metric = metric;
        this.value = value;
    }

    public NumericData(NumericMetric metric, UUID timeUUID, double value, Set<Tag> tags) {
        super(metric, timeUUID, tags);
        this.value = value;
    }

    public NumericData(NumericMetric metric, UUID timeUUID, double value, Set<Tag> tags, Long writeTime) {
        super(metric, timeUUID, tags, writeTime);
    }

    public NumericData(long timestamp, double value) {
        super(timestamp);
        this.value = value;
    }

    public NumericData(UUID timeUUID, double value) {
        super(timeUUID);
        this.value = value;
    }

    public NumericData(UUID timeUUID, double value, Set<Tag> tags) {
        super(timeUUID, tags);
        this.value = value;
    }

    public NumericData(UUID timeUUID, double value, Set<Tag> tags, Long writeTime) {
        super(timeUUID, tags, writeTime);
        this.value = value;
    }

    /**
     * Currently not used. It wil be used for breaking up a metric time series into multiple partitions by date. For
     * example, if we choose to partition by month, then all data collected for a given metric during October would go
     * into one partition, and data collected during November would go into another partition. This will not impact
     * writes, but it will make reads more complicated and potentially more expensive if we make the date partition too
     * small.
     */

    public Metric getMetric() {
        return metric;
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
            .add("metric", metric)
            .toString();
    }
}
