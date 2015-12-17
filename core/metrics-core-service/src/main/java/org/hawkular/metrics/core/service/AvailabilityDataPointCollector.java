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

import static org.hawkular.metrics.model.AvailabilityType.DOWN;
import static org.hawkular.metrics.model.AvailabilityType.UP;

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
    private long downtimeDuration;
    private long lastDowntime;
    private long downtimeCount;

    AvailabilityDataPointCollector(Buckets buckets, int bucketIndex) {
        this.buckets = buckets;
        bucketStart = buckets.getBucketStart(bucketIndex);
    }

    void increment(DataPoint<AvailabilityType> dataPoint) {
        long timestamp = dataPoint.getTimestamp();
        AvailabilityType value = dataPoint.getValue();

        if (previous != null && timestamp <= previous.getTimestamp()) {
            throw new IllegalStateException("Expected stream sorted in time ascending order");
        }

        if (previous == null) {
            if (value == DOWN) {
                downtimeDuration += timestamp - bucketStart;
                lastDowntime = timestamp;
                downtimeCount++;
            }
        } else {
            if (value == DOWN) {
                lastDowntime = timestamp;
                if (previous.getValue() == DOWN) {
                    downtimeDuration += timestamp - previous.getTimestamp();
                } else {
                    downtimeCount++;
                }
            } else if (value == UP) {
                if (previous.getValue() == DOWN) {
                    downtimeDuration += timestamp - previous.getTimestamp();
                    lastDowntime = timestamp;
                }
            }
        }

        previous = dataPoint;
    }

    AvailabilityBucketPoint toBucketPoint() {
        long to = bucketStart + buckets.getStep();

        if (previous.getValue() == DOWN) {
            downtimeDuration += to - previous.getTimestamp();
            lastDowntime = to;
        }

        return new AvailabilityBucketPoint.Builder(bucketStart, to)
                .setDowntimeDuration(downtimeDuration)
                .setLastDowntime(lastDowntime)
                .setUptimeRatio(1.0 - (double) downtimeDuration / buckets.getStep())
                .setDowntimeCount(downtimeCount)
                .build();
    }
}
