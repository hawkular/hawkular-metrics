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

/**
 * Statistics for gauge data in a time range.
 *
 * @author Heiko W. Rupp
 */
public class GaugeBucketDataPoint {
    private long start;
    private long end;
    private double value;
    private double min;
    private double avg;
    private double median;
    private double max;
    private double percentile95th;

    public GaugeBucketDataPoint(
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

    public long getStart() {
        return start;
    }

    public long getEnd() {
        return end;
    }

    public double getValue() {
        return value;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public double getAvg() {
        return avg;
    }

    public double getMedian() {
        return median;
    }

    public double getPercentile95th() {
        return percentile95th;
    }

    public boolean isEmpty() {
        return isNaN(min) || isNaN(avg) || isNaN(median) || isNaN(max) || isNaN(percentile95th);
    }

    @Override
    public String toString() {
        return "GuageBucketDataPoint[" +
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
     * Create {@link GaugeBucketDataPoint} instances following the builder pattern.
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

        public GaugeBucketDataPoint build() {
            return new GaugeBucketDataPoint(start, end, value, min, avg, median, max, percentile95th);
        }
    }
}
