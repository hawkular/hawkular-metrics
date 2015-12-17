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
package org.hawkular.metrics.core.service;

import org.apache.commons.math3.stat.descriptive.summary.Sum;
import org.hawkular.metrics.model.NumericBucketPoint;

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
    private Sum samples = new Sum();
    private Long start;
    private Long end;

    SumNumericBucketPointCollector() {
    }

    void increment(NumericBucketPoint bucketPoint) {
        min.increment(bucketPoint.getMin());
        average.increment(bucketPoint.getAvg());
        median.increment(bucketPoint.getMedian());
        max.increment(bucketPoint.getMax());
        samples.increment(1);

        start = bucketPoint.getStart();
        end = bucketPoint.getEnd();
    }

    NumericBucketPoint toBucketPoint() {
        int localSamples = Integer.MAX_VALUE;
        if (samples.getN() >= Integer.MIN_VALUE && samples.getN() <= Integer.MAX_VALUE) {
            localSamples = (int) samples.getN();
        }

        return new NumericBucketPoint.Builder(start, end)
                .setMin(min.getResult())
                .setAvg(average.getResult())
                .setMedian(median.getResult())
                .setMax(max.getResult())
                .setSamples(localSamples)
                .build();
    }
}
