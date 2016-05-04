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

package org.hawkular.metrics.core.service.transformers;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.rank.Max;
import org.apache.commons.math3.stat.descriptive.rank.Min;
import org.apache.commons.math3.stat.descriptive.rank.PSquarePercentile;
import org.apache.commons.math3.stat.descriptive.summary.Sum;
import org.hawkular.metrics.core.service.PercentileWrapper;
import org.hawkular.metrics.model.Buckets;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.NumericBucketPoint;
import org.hawkular.metrics.model.Percentile;

/**
 * Accumulates numeric data points to produce a {@link NumericBucketPoint}.
 *
 * @author Thomas Segismont
 */
public final class NumericDataPointCollector {

    /**
     * This is a test hook. See {@link Percentile} for details.
     */
    public static Function<Double, PercentileWrapper> createPercentile = p -> new PercentileWrapper() {

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

    private int samples = 0;
    private Min min = new Min();
    private Mean average = new Mean();
    private PercentileWrapper median = createPercentile.apply(50.0);
    private Max max = new Max();
    private Sum sum = new Sum();
    private List<PSquarePercentile> percentiles;

    public NumericDataPointCollector(Buckets buckets, int bucketIndex, List<Double> percentileList) {
        this.buckets = buckets;
        this.bucketIndex = bucketIndex;
        this.percentiles = new ArrayList<>();
        percentileList.stream().forEach(d -> percentiles.add(new PSquarePercentile(d)));
    }

    public void increment(DataPoint<? extends Number> dataPoint) {
        Number value = dataPoint.getValue();
        min.increment(value.doubleValue());
        average.increment(value.doubleValue());
        median.addValue(value.doubleValue());
        max.increment(value.doubleValue());
        sum.increment(value.doubleValue());
        samples++;
        percentiles.stream().forEach(p -> p.increment(value.doubleValue()));
    }

    public NumericBucketPoint toBucketPoint() {
        long from = buckets.getBucketStart(bucketIndex);
        long to = from + buckets.getStep();
        return new NumericBucketPoint.Builder(from, to)
                .setMin(min.getResult())
                .setAvg(average.getResult())
                .setMedian(median.getResult())
                .setMax(max.getResult())
                .setSum(sum.getResult())
                .setSamples(samples)
                .setPercentiles(percentiles.stream()
                        .map(p -> new Percentile(p.quantile(), p.getResult())).collect(Collectors.toList()))
                .build();
    }
}
