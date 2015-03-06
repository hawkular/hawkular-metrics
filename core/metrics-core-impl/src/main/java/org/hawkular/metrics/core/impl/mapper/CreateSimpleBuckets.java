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
package org.hawkular.metrics.core.impl.mapper;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summarizingDouble;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.NumericData;
import org.hawkular.metrics.core.api.NumericMetric;

public class CreateSimpleBuckets extends MetricMapper<BucketedOutput> {

    private long startTime;
    private long endTime;
    private int numberOfBuckets;
    private boolean skipEmpty;

    public CreateSimpleBuckets(long startTime, long endTime, int numberOfBuckets, boolean skipEmpty) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.numberOfBuckets = numberOfBuckets;
        this.skipEmpty = skipEmpty;
    }

    @Override
    public BucketedOutput doApply(NumericMetric metric) {
        // we will have numberOfBuckets buckets over the whole time span
        BucketedOutput output = new BucketedOutput(metric.getTenantId(), metric.getId().getName(), metric.getTags());
        long bucketSize = (endTime - startTime) / numberOfBuckets;

        long[] buckets = LongStream.iterate(0, i -> i + 1).limit(numberOfBuckets)
            .map(i -> startTime + (i * bucketSize)).toArray();

        Map<Long, List<NumericData>> map = metric.getData().stream()
                .collect(groupingBy(dataPoint -> findBucket(buckets, bucketSize, dataPoint.getTimestamp())));

        Map<Long, DoubleSummaryStatistics> statsMap = map.entrySet().stream()
            .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().stream().collect(summarizingDouble(
                NumericData::getValue))));

        for (Long bucket : buckets) {
            statsMap.computeIfAbsent(bucket, key -> new DoubleSummaryStatistics());
        }

        output.setData(statsMap.entrySet().stream()
            .sorted((left, right) -> left.getKey().compareTo(right.getKey()))
            .filter(e -> !skipEmpty || e.getValue().getCount() > 0)
                .map(e -> getBucketedDataPoint(metric.getId(), e.getKey(), e.getValue()))
            .collect(toList()));

        return output;
    }

    static long findBucket(long[] buckets, long bucketSize, long timestamp) {
        return stream(buckets).filter(bucket -> timestamp >= bucket && timestamp < bucket + bucketSize)
            .findFirst().getAsLong();
    }

    static BucketDataPoint getBucketedDataPoint(MetricId id, long timestamp, DoubleSummaryStatistics stats) {
        // Currently, if a bucket does not contain any data, we set max/min/avg to Double.NaN.
        // DoubleSummaryStatistics however uses Double.Infinity for max/min and 0.0 for avg.
        if (stats.getCount() > 0) {
            return new BucketDataPoint(id.getName(), timestamp, stats.getMin(), stats.getAverage(), stats.getMax());
        }
        return new BucketDataPoint(id.getName(), timestamp, Double.NaN, Double.NaN, Double.NaN);
    }
}