/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.service;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.math3.stat.descriptive.summary.Sum;
import org.hawkular.metrics.model.NumericBucketPoint;
import org.hawkular.metrics.model.Percentile;

/**
 * Accumulates numeric data points to produce a {@link NumericBucketPoint}.
 *
 * @author Stefan Negrea
 */
final class SumNumericBucketPointCollector {

    private Sum min = new Sum();
    private Sum average = new Sum();
    private Sum median = new Sum();
    private Sum max = new Sum();
    private Sum sum = new Sum();
    private Sum samples = new Sum();
    private Map<String, Sum> percentiles = new HashMap<>();
    private Long start;
    private Long end;

    SumNumericBucketPointCollector() {
    }

    void increment(NumericBucketPoint bucketPoint) {
        if (!bucketPoint.isEmpty()) {
            min.increment(bucketPoint.getMin());
            average.increment(bucketPoint.getAvg());
            median.increment(bucketPoint.getMedian());
            max.increment(bucketPoint.getMax());
            sum.increment(bucketPoint.getSum());
            samples.increment(1);
            if (bucketPoint.getPercentiles() != null) {
                for (Percentile p : bucketPoint.getPercentiles()) {
                    percentiles.computeIfAbsent(p.getOriginalQuantile(), k -> new Sum())
                            .increment(p.getValue());
                }
            }
        }

        start = bucketPoint.getStart();
        end = bucketPoint.getEnd();
    }

    NumericBucketPoint toBucketPoint() {
        int localSamples = Integer.MAX_VALUE;
        if (samples.getN() >= Integer.MIN_VALUE && samples.getN() <= Integer.MAX_VALUE) {
            localSamples = (int) samples.getN();
        }

        NumericBucketPoint.Builder builder = new NumericBucketPoint.Builder(start, end)
                .setMin(min.getResult())
                .setAvg(average.getResult())
                .setMedian(median.getResult())
                .setMax(max.getResult())
                .setSum(sum.getResult())
                .setSamples(localSamples);

        if (!percentiles.isEmpty()) {
            builder.setPercentiles(percentiles.entrySet().stream()
                    .map(e -> new Percentile(e.getKey(), e.getValue().getResult()))
                    .collect(Collectors.toList()));
        }

        return builder.build();
    }
}
