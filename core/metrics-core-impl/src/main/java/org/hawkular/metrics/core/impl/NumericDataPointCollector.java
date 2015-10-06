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
package org.hawkular.metrics.core.impl;

import java.util.function.Function;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.rank.Max;
import org.apache.commons.math3.stat.descriptive.rank.Min;
import org.apache.commons.math3.stat.descriptive.rank.PSquarePercentile;
import org.hawkular.metrics.core.api.Buckets;
import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.NumericBucketPoint;

/**
 * Accumulates numeric data points to produce a {@link NumericBucketPoint}.
 *
 * @author Thomas Segismont
 */
final class NumericDataPointCollector {

    /**
     * This is a test hook. See {@link Percentile} for details.
     */
    static Function<Double, Percentile> createPercentile = p -> new Percentile() {

        PSquarePercentile percentile = new PSquarePercentile(p);

        // Note that these methods need to be synchronized because PSquarePecentile is not synchronized and we need to
        // support concurrent access.

        @Override
        public synchronized void addValue(double value) {
            percentile.increment(value);
        }

        @Override
        public synchronized double getResult() {
            return percentile.getResult();
        }
    };

    private final Buckets buckets;
    private final int bucketIndex;

    private Min min = new Min();
    private Mean average = new Mean();
    private Percentile median = createPercentile.apply(50.0);
    private Max max = new Max();
    private Percentile percentile95th = createPercentile.apply(95.0);

    NumericDataPointCollector(Buckets buckets, int bucketIndex) {
        this.buckets = buckets;
        this.bucketIndex = bucketIndex;
    }

    void increment(DataPoint<? extends Number> dataPoint) {
        Number value = dataPoint.getValue();
        min.increment(value.doubleValue());
        average.increment(value.doubleValue());
        median.addValue(value.doubleValue());
        max.increment(value.doubleValue());
        percentile95th.addValue(value.doubleValue());
    }

    NumericBucketPoint toBucketPoint() {
        long from = buckets.getBucketStart(bucketIndex);
        long to = from + buckets.getStep();
        return new NumericBucketPoint.Builder(from, to)
                .setMin(min.getResult())
                .setAvg(average.getResult())
                .setMedian(median.getResult())
                .setMax(max.getResult())
                .setPercentile95th(percentile95th.getResult())
                .build();
    }
}
