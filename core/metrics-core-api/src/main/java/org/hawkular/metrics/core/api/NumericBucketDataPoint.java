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

import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;

import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

/**
 * Statistics for numeric data in a time range.
 *
 * @author Heiko W. Rupp
 */
@ApiModel(value = "Statistics for numeric data in a time range.")
public class NumericBucketDataPoint {
    private long timestamp;
    private double value;
    private double min;
    private double avg;
    private double median;
    private double max;
    private double percentile95th;

    public static NumericBucketDataPoint newEmptyInstance(long timestamp) {
        NumericBucketDataPoint dataPoint = new NumericBucketDataPoint();
        dataPoint.setTimestamp(timestamp);
        dataPoint.setMin(NaN);
        dataPoint.setAvg(NaN);
        dataPoint.setMedian(NaN);
        dataPoint.setMax(NaN);
        dataPoint.setPercentile95th(NaN);
        return dataPoint;
    }

    @ApiModelProperty(value = "Time when the value was obtained in milliseconds since epoch")
    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @ApiModelProperty(value = "The value of this data point")
    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @ApiModelProperty(value = "Minimum value during the time span of the bucket.")
    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    @ApiModelProperty(value = "Maximum value during the time span of the bucket.")
    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    @ApiModelProperty(value = "Average value during the time span of the bucket.")
    public double getAvg() {
        return avg;
    }

    public void setAvg(double avg) {
        this.avg = avg;
    }

    @ApiModelProperty(value = "Median value during the time span of the bucket.")
    public double getMedian() {
        return median;
    }

    public void setMedian(double median) {
        this.median = median;
    }

    @ApiModelProperty(value = "95th percentile value during the time span of the bucket.")
    public double getPercentile95th() {
        return percentile95th;
    }

    public void setPercentile95th(double percentile95th) {
        this.percentile95th = percentile95th;
    }

    public boolean isEmpty() {
        return isNaN(min) || isNaN(avg) || isNaN(median) || isNaN(max) || isNaN(percentile95th);
    }

    @Override
    public String toString() {
        return "NumericBucketDataPoint[" +
               "timestamp=" + timestamp +
               ", min=" + min +
               ", avg=" + avg +
               ", median=" + median +
               ", max=" + max +
               ", percentile95th=" + percentile95th +
               ']';
    }
}
