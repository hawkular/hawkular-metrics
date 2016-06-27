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
    private Max max = new Max();
    private Sum sum = new Sum();
    private List<PercentileWrapper> percentiles;
    private List<Percentile> percentileList;

    public NumericDataPointCollector(Buckets buckets, int bucketIndex, List<Percentile> percentilesList) {
        this.buckets = buckets;
        this.bucketIndex = bucketIndex;
        this.percentiles = new ArrayList<>(percentilesList.size() + 1);
        this.percentileList = percentilesList;
        percentilesList.stream().forEach(d -> percentiles.add(createPercentile.apply(d.getQuantile())));
        percentiles.add(createPercentile.apply(50.0)); // Important to be the last one
    }

    public void increment(DataPoint<? extends Number> dataPoint) {
        Number value = dataPoint.getValue();
        min.increment(value.doubleValue());
        average.increment(value.doubleValue());
        max.increment(value.doubleValue());
        sum.increment(value.doubleValue());
        samples++;
        percentiles.stream().forEach(p -> p.addValue(value.doubleValue()));
    }

    public NumericBucketPoint toBucketPoint() {
        long from = buckets.getBucketStart(bucketIndex);
        long to = from + buckets.getStep();

        // Original percentilesList can't be modified as it is used elsewhere
        List<Percentile> percentileReturns = new ArrayList<>(percentileList.size());

        if(percentileList.size() > 0) {
            for(int i = 0; i < percentileList.size(); i++) {
                Percentile p = percentileList.get(i);
                PercentileWrapper pw = percentiles.get(i);
                percentileReturns.add(new Percentile(p.getOriginalQuantile(), pw.getResult()));
            }
        }

        return new NumericBucketPoint.Builder(from, to)
                .setMin(min.getResult())
                .setAvg(average.getResult())
                .setMedian(this.percentiles.get(this.percentiles.size() - 1).getResult())
                .setMax(max.getResult())
                .setSum(sum.getResult())
                .setSamples(samples)
                .setPercentiles(percentileReturns)
                .build();
    }
}
