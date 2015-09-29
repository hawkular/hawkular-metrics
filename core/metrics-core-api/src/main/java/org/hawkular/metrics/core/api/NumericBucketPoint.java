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

import java.util.List;
import java.util.Map;

/**
 * {@link BucketPoint} for numeric metrics.
 *
 * @author Thomas Segismont
 */
public final class NumericBucketPoint extends BucketPoint {
    private final double min;
    private final double avg;
    private final double median;
    private final double max;
    private final double percentile95th;

    private NumericBucketPoint(long start, long end, double min, double avg, double median, double max, double
            percentile95th) {
        super(start, end);
        this.min = min;
        this.avg = avg;
        this.median = median;
        this.max = max;
        this.percentile95th = percentile95th;
    }

    public double getMin() {
        return min;
    }

    public double getAvg() {
        return avg;
    }

    public double getMedian() {
        return median;
    }

    public double getMax() {
        return max;
    }

    public double getPercentile95th() {
        return percentile95th;
    }

    @Override
    public boolean isEmpty() {
        return isNaN(min) || isNaN(avg) || isNaN(median) || isNaN(max) || isNaN(percentile95th);
    }

    @Override
    public String toString() {
        return "NumericBucketPoint[" +
                "start=" + getStart() +
                ", end=" + getEnd() +
                ", min=" + min +
                ", avg=" + avg +
                ", median=" + median +
                ", max=" + max +
                ", percentile95th=" + percentile95th +
                ", isEmpty=" + isEmpty() +
                ']';
    }

    /**
     * @see BucketPoint#toList(Map, Buckets, java.util.function.BiFunction)
     */
    public static List<NumericBucketPoint> toList(Map<Long, NumericBucketPoint> pointMap, Buckets buckets) {
        return BucketPoint.toList(pointMap, buckets, (start, end) -> new Builder(start, end).build());
    }

    public static class Builder {
        private final long start;
        private final long end;
        private double min = NaN;
        private double avg = NaN;
        private double median = NaN;
        private double max = NaN;
        private double percentile95th = NaN;

        /**
         * Creates a builder for an initially empty instance, configurable with the builder setters.
         *
         * @param start the start timestamp of this bucket point
         * @param end   the end timestamp of this bucket point
         */
        public Builder(long start, long end) {
            this.start = start;
            this.end = end;
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

        public NumericBucketPoint build() {
            return new NumericBucketPoint(start, end, min, avg, median, max, percentile95th);
        }
    }

}
