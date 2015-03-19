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
    private long start;
    private long end;
    private double value;
    private double min;
    private double avg;
    private double median;
    private double max;
    private double percentile95th;

    public NumericBucketDataPoint(
            long start,
            long end,
            double value,
            double min,
            double avg,
            double median,
            double max,
            double percentile95th
    ) {
        this.start = start;
        this.end = end;
        this.value = value;
        this.min = min;
        this.avg = avg;
        this.median = median;
        this.max = max;
        this.percentile95th = percentile95th;
    }

    @ApiModelProperty(value = "Start timestamp of this bucket in milliseconds since epoch")
    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    @ApiModelProperty(value = "End timestamp of this bucket in milliseconds since epoch")
    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
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
               "start=" + start +
               ", end=" + end +
               ", min=" + min +
               ", avg=" + avg +
               ", median=" + median +
               ", max=" + max +
               ", percentile95th=" + percentile95th +
               ']';
    }

    /**
     * Create {@link NumericBucketDataPoint} instances following the builder pattern.
     */
    public static class Builder {
        private final long start;
        private final long end;
        private double value = NaN;
        private double min = NaN;
        private double avg = NaN;
        private double median = NaN;
        private double max = NaN;
        private double percentile95th = NaN;

        /**
         * Creates a builder for an initially empty instance, configurable with the builder setters.
         *
         * @param start the start timestamp of this bucket data point
         * @param end   the end timestamp of this bucket data point
         */
        public Builder(long start, long end) {
            this.start = start;
            this.end = end;
        }

        public Builder setValue(double value) {
            this.value = value;
            return this;
        }

        public Builder setMin(double min) {
            this.min = min;
            return this;
        }

        public Builder setAvg(double avg) {
            this.avg = avg;
            return this;
        }

        public Builder setMedian(double median) {
            this.median = median;
            return this;
        }

        public Builder setMax(double max) {
            this.max = max;
            return this;
        }

        public Builder setPercentile95th(double percentile95th) {
            this.percentile95th = percentile95th;
            return this;
        }

        public NumericBucketDataPoint build() {
            return new NumericBucketDataPoint(start, end, value, min, avg, median, max, percentile95th);
        }
    }
}
