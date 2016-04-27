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

import static java.lang.Double.NaN;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@link BucketPoint} for numeric metrics.
 *
 * @author Thomas Segismont
 */
public class NumericBucketPoint extends BucketPoint {
    private final Double min;
    private final Double avg;
    private final Double median;
    private final Double max;
    private final Double sum;
    private Integer samples;
    private final List<Percentile> percentiles;

    private NumericBucketPoint(long start, long end, double min, double avg, double median, double max, double sum,
            List<Percentile> percentiles, int samples) {
        super(start, end);
        this.min = getDoubleValue(min);
        this.avg = getDoubleValue(avg);
        this.median = getDoubleValue(median);
        this.max = getDoubleValue(max);
        this.sum = getDoubleValue(sum);
        this.percentiles = percentiles;
        if (samples != 0) {
            this.samples = samples;
        }
    }

    public Double getMin() {
        return min;
    }

    public Double getAvg() {
        return avg;
    }

    public Double getMedian() {
        return median;
    }

    public Double getMax() {
        return max;
    }

    public Double getSum() {
        return sum;
    }

    public List<Percentile> getPercentiles() {
        return percentiles;
    }

    public Integer getSamples() {
        return samples;
    }

    @Override
    public boolean isEmpty() {
        return samples == null || min == null || avg == null || median == null || max == null || sum == null;
//        return samples == null || isNaN(min) || isNaN(avg) || isNaN(median) || isNaN(max) || isNaN(sum);
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
                ", sum=" + sum +
                ", percentiles=" + percentiles +
                ", samples=" + samples +
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
        private double sum = NaN;
        private List<Percentile> percentiles = new ArrayList<>();
        private int samples = 0;

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

        public Builder(NumericBucketPoint numericBucketPoint) {
            this.start = numericBucketPoint.getStart();
            this.end = numericBucketPoint.getEnd();
            this.setMin(numericBucketPoint.getMin());
            this.setAvg(numericBucketPoint.getAvg());
            this.setMedian(numericBucketPoint.getMedian());
            this.setMax(numericBucketPoint.getMax());
            this.setSum(numericBucketPoint.getSum());
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

        public Builder setSum(double sum) {
            this.sum = sum;
            return this;
        }

        public Builder setPercentiles(List<Percentile> percentiles) {
            this.percentiles = percentiles;
            return this;
        }

        public Builder setSamples(int samples) {
            this.samples = samples;
            return this;
        }

        public NumericBucketPoint build() {
            return new NumericBucketPoint(start, end, min, avg, median, max, sum, percentiles, samples);
        }
    }

}
