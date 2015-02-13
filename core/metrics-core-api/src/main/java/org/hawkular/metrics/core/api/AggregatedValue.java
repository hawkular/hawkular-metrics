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

import java.util.UUID;

import com.google.common.base.Objects;

/**
 * An aggregated numeric value. This value is the result of applying a function to data from one or more metrics. The
 * input data can itself be raw or aggregated.
 *
 * @author John Sanda
 */
public class AggregatedValue {

    private final String type;

    private String srcMetric;

    private Interval srcMetricInterval;

    private final double value;

    private UUID timeUUID;

    public AggregatedValue(String type, double value) {
        this.type = type;
        this.value = value;
    }

    public AggregatedValue(String type, double value, String srcMetric, Interval srcMetricInterval, UUID timeUUID) {
        this.type = type;
        this.value = value;
        this.srcMetric = srcMetric;
        this.srcMetricInterval = srcMetricInterval;
        this.timeUUID = timeUUID;
    }

    /**
     * The aggregation function used to produce this value, e.g., max, min, sum,
     * count, etc. <br>
     * <br>
     * <strong>Note:</strong> Once we have more functions support in place, this
     * will likely be replaced by a more strongly typed object.
     *
     * @return type
     */
    public String getType() {
        return type;
    }

    /**
     * When the aggregated value is the result of applying a function to data
     * from multiple metrics, this might be set for certain functions. For
     * example, if we are computing the max or min from a set of data points
     * from multiple metrics, then <code>srcMetric</code> along with
     * {@link #srcMetricInterval} will tell us from which metric that value
     * came.
     *
     * @return source metrics
     */
    public String getSrcMetric() {
        return srcMetric;
    }

    /**
     * When the aggregated value is the result of applying a function to data
     * from multiple metrics, this might be set for particular functions. For
     * example, if we are computing the max or min from a set of data points
     * from multiple metrics, <code>srcMetricInterval</code> along with
     * {@link #srcMetric} will tell us from which metric that value came.
     *
     * @return source metric interval
     */
    public Interval getSrcMetricInterval() {
        return srcMetricInterval;
    }

    public double getValue() {
        return value;
    }

    /**
     * This will be set for certain functions that basically perform filtering.
     * For example, if we are computing the max or min, then
     * <code>timestamp</code> gives us the collection time of the input data
     * point that is the max or min.
     *
     * @return time UUID
     */
    public UUID getTimeUUID() {
        return timeUUID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AggregatedValue that = (AggregatedValue) o;

        if (Double.compare(that.value, value) != 0) return false;
        if (srcMetric != null ? !srcMetric.equals(that.srcMetric) : that.srcMetric != null) return false;
        if (srcMetricInterval != null ? !srcMetricInterval.equals(that.srcMetricInterval) :
            that.srcMetricInterval != null)
            return false;
        if (timeUUID != null ? !timeUUID.equals(that.timeUUID) : that.timeUUID != null) return false;
        if (!type.equals(that.type)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = type.hashCode();
        result = 31 * result + (srcMetric != null ? srcMetric.hashCode() : 0);
        result = 31 * result + (srcMetricInterval != null ? srcMetricInterval.hashCode() : 0);
        temp = Double.doubleToLongBits(value);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (timeUUID != null ? timeUUID.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("type", type)
            .add("value", value)
            .add("srcMetric", srcMetric)
            .add("srcMetricInterval", srcMetricInterval)
            .add("timestamp", timeUUID)
            .toString();
    }

}
