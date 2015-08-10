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

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.rank.Max;
import org.apache.commons.math3.stat.descriptive.rank.Min;
import org.apache.commons.math3.stat.descriptive.rank.PSquarePercentile;
import org.hawkular.metrics.core.api.Buckets;
import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.GaugeBucketPoint;

/**
 * Accumulates gauge data points to produce a {@link GaugeBucketPoint}.
 *
 * @author Thomas Segismont
 */
final class GaugeDataPointCollector {
    private final Buckets buckets;
    private final int bucketIndex;

    private Min min = new Min();
    private Mean average = new Mean();
    private PSquarePercentile median = new PSquarePercentile(50.0);
    private Max max = new Max();
    private PSquarePercentile percentile95th = new PSquarePercentile(95.0);

    GaugeDataPointCollector(Buckets buckets, int bucketIndex) {
        this.buckets = buckets;
        this.bucketIndex = bucketIndex;
    }

    void increment(DataPoint<Double> dataPoint) {
        Double value = dataPoint.getValue();
        min.increment(value);
        average.increment(value);
        median.increment(value);
        max.increment(value);
        percentile95th.increment(value);
    }

    GaugeBucketPoint toBucketPoint() {
        long from = buckets.getBucketStart(bucketIndex);
        long to = from + buckets.getStep();
        return new GaugeBucketPoint.Builder(from, to)
                .setMin(min.getResult())
                .setAvg(average.getResult())
                .setMedian(median.getResult())
                .setMax(max.getResult())
                .setPercentile95th(percentile95th.getResult())
                .build();
    }
}
