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
import org.hawkular.metrics.core.api.GaugeBucketDataPoint;

/**
 * @author Thomas Segismont
 */
final class GaugeBucketAggregate {
    private final Buckets buckets;
    private final int bucketIndex;

    private boolean isEmpty = true;
    private Min min = new Min();
    private Mean average = new Mean();
    private PSquarePercentile median = new PSquarePercentile(50.0);
    private Max max = new Max();
    private PSquarePercentile percentile95th = new PSquarePercentile(95.0);

    GaugeBucketAggregate(Buckets buckets, int bucketIndex) {
        this.buckets = buckets;
        this.bucketIndex = bucketIndex;
    }

    public void increment(DataPoint<Double> dataPoint) {
        isEmpty = false;
        Double value = dataPoint.getValue();
        min.increment(value);
        average.increment(value);
        median.increment(value);
        max.increment(value);
        percentile95th.increment(value);
    }

    public GaugeBucketDataPoint toBucketPoint() {
        long from = buckets.getRangeStart(bucketIndex);
        long to = from + buckets.getStep();
        GaugeBucketDataPoint.Builder builder = new GaugeBucketDataPoint.Builder(from, to);
        if (!isEmpty) {
            builder.setMin(min.getResult())
                    .setAvg(average.getResult())
                    .setMedian(median.getResult())
                    .setMax(max.getResult())
                    .setPercentile95th(percentile95th.getResult());
        }
        return builder.build();
    }
}
