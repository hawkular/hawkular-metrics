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

import static org.hawkular.metrics.core.api.AvailabilityType.DOWN;

import java.util.List;

import org.hawkular.metrics.core.api.AvailabilityBucketDataPoint;
import org.hawkular.metrics.core.api.AvailabilityType;
import org.hawkular.metrics.core.api.Buckets;
import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.MetricId;

/**
 * @author Thomas Segismont
 */
public class AvailabilityBucketedOutputMapper
        extends BucketedOutputMapper<AvailabilityType, AvailabilityBucketDataPoint> {

    /**
     * @param tenantId the tenant
     * @param id the id
     * @param buckets the bucket configuration
     */
    public AvailabilityBucketedOutputMapper(String tenantId, MetricId id, Buckets buckets) {
        super(tenantId, id, buckets);
    }

    @Override
    protected AvailabilityBucketDataPoint newEmptyPointInstance(long from, long to) {
        return new AvailabilityBucketDataPoint.Builder(from, to).build();
    }

    @Override
    protected AvailabilityBucketDataPoint newPointInstance(long from, long to,
            List<DataPoint<AvailabilityType>> availabilities) {
        long downtimeDuration = 0, lastDowntime = 0, downtimeCount = 0;

        for (int i = 0; i < availabilities.size(); i++) {
            DataPoint<AvailabilityType> availability = availabilities.get(i);
            long leftTimestamp = i == 0 ? from : availability.getTimestamp();
            long rightTimestamp = i == availabilities.size() - 1 ? to : availabilities.get(i + 1).getTimestamp();

            if (availability.getValue() == DOWN) {
                downtimeDuration += rightTimestamp - leftTimestamp;
                lastDowntime = rightTimestamp;
                downtimeCount++;
            }
        }

        return new AvailabilityBucketDataPoint.Builder(from, to)
                .setDowntimeDuration(downtimeDuration)
                .setLastDowntime(lastDowntime)
                .setUptimeRatio(1.0 - (double) downtimeDuration / buckets.getStep())
                .setDowntimeCount(downtimeCount)
                .build();
    }
}
