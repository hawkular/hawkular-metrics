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

import static org.hawkular.metrics.model.AvailabilityType.UP;

import java.util.HashMap;
import java.util.Map;

import org.hawkular.metrics.model.AvailabilityBucketPoint;
import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.Buckets;
import org.hawkular.metrics.model.DataPoint;

/**
 * Accumulates availability data points to produce an {@link AvailabilityBucketPoint}.
 *
 * @author Thomas Segismont
 */
final class AvailabilityDataPointCollector {

    private final Buckets buckets;
    private final long bucketStart;

    private DataPoint<AvailabilityType> previous;
    private Map<AvailabilityType, Long> durationMap;
    private long lastNotUptime;
    private long notUpCount;
    private long samples;

    AvailabilityDataPointCollector(Buckets buckets, int bucketIndex) {
        this.buckets = buckets;
        this.bucketStart = buckets.getBucketStart(bucketIndex);

        this.durationMap = new HashMap<>();
    }

    void increment(DataPoint<AvailabilityType> dataPoint) {
        long timestamp = dataPoint.getTimestamp();
        AvailabilityType availType = dataPoint.getValue();

        if (previous != null && timestamp <= previous.getTimestamp()) {
            throw new IllegalStateException("Expected stream sorted in time ascending order");
        }

        ++samples;

        if (previous == null) {
            Long availTypeDuration = durationMap.getOrDefault(availType, 0L);
            availTypeDuration += (timestamp - bucketStart);
            durationMap.put(availType, availTypeDuration);
            if (availType != UP) {
                lastNotUptime = timestamp;
                ++notUpCount;
            }
        }
        else {
            Long previousAvailTypeDuration = durationMap.getOrDefault(previous.getValue(), 0L);
            previousAvailTypeDuration += (timestamp - previous.getTimestamp());
            durationMap.put(previous.getValue(), previousAvailTypeDuration);

            if (availType == UP) {
                if (previous.getValue() != UP) {
                    lastNotUptime = timestamp;
                }
            } else {
                if (previous.getValue() == UP) {
                    ++notUpCount;
                }
                lastNotUptime = timestamp;
            }
        }

        previous = dataPoint;
    }

    AvailabilityBucketPoint toBucketPoint() {
        long to = bucketStart + buckets.getStep();

        Long availTypeDuration = durationMap.getOrDefault(previous.getValue(), 0L);
        availTypeDuration += (to - previous.getTimestamp());
        durationMap.put(previous.getValue(), availTypeDuration);

        if (previous.getValue() != UP) {
            lastNotUptime = to;
        }

        return new AvailabilityBucketPoint.Builder(bucketStart, to)
                .setDurationMap(durationMap)
                .setLastNotUptime(lastNotUptime)
                .setUptimeRatio(((double) durationMap.getOrDefault(AvailabilityType.UP, 0L) / buckets.getStep()))
                .setNotUptimeCount(notUpCount)
                .setSamples(samples)
                .build();
    }
}
