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
package org.hawkular.metrics.models;

import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;

import java.util.List;
import java.util.Map;

/**
 * {@link BucketPoint} for availability metrics.
 *
 * @author Thomas Segismont
 */
public final class AvailabilityBucketPoint extends BucketPoint {
    private final long downtimeDuration;
    private final long lastDowntime;
    private final double uptimeRatio;
    private final long downtimeCount;

    protected AvailabilityBucketPoint(long start, long end, long downtimeDuration, long lastDowntime, double
            uptimeRatio, long downtimeCount) {
        super(start, end);
        this.downtimeDuration = downtimeDuration;
        this.lastDowntime = lastDowntime;
        this.uptimeRatio = uptimeRatio;
        this.downtimeCount = downtimeCount;
    }

    public long getDowntimeDuration() {
        return downtimeDuration;
    }

    public long getLastDowntime() {
        return lastDowntime;
    }

    public double getUptimeRatio() {
        return uptimeRatio;
    }

    public long getDowntimeCount() {
        return downtimeCount;
    }

    @Override
    public boolean isEmpty() {
        return isNaN(uptimeRatio);
    }

    @Override
    public String toString() {
        return "AvailabilityBucketPoint[" +
                "start=" + getStart() +
                ", end=" + getEnd() +
                ", downtimeDuration=" + downtimeDuration +
                ", lastDowntime=" + lastDowntime +
                ", uptimeRatio=" + uptimeRatio +
                ", downtimeCount=" + downtimeCount +
                ", isEmpty=" + isEmpty() +
                ']';
    }

    /**
     * @see BucketPoint#toList(Map, Buckets, java.util.function.BiFunction)
     */
    public static List<AvailabilityBucketPoint> toList(Map<Long, AvailabilityBucketPoint> pointMap, Buckets buckets) {
        return BucketPoint.toList(pointMap, buckets, (start, end) -> new Builder(start, end).build());
    }

    public static class Builder {
        private final long start;
        private final long end;
        private long downtimeDuration = 0;
        private long lastDowntime = 0;
        private double uptimeRatio = NaN;
        private long downtimeCount = 0;

        /**
         * Creates a builder for an initially empty instance, configurable with the builder setters.
         *
         * @param start the start timestamp of this bucket point
         * @param end   the end timestamp of this bucket point
         */
        public Builder(long start, long end) {
            this.start = start;
            this.end = end;
        }

        public Builder setDowntimeDuration(long downtimeDuration) {
            this.downtimeDuration = downtimeDuration;
            return this;
        }

        public Builder setLastDowntime(long lastDowntime) {
            this.lastDowntime = lastDowntime;
            return this;
        }

        public Builder setUptimeRatio(double uptimeRatio) {
            this.uptimeRatio = uptimeRatio;
            return this;
        }

        public Builder setDowntimeCount(long downtimeCount) {
            this.downtimeCount = downtimeCount;
            return this;
        }

        public AvailabilityBucketPoint build() {
            return new AvailabilityBucketPoint(start, end, downtimeDuration, lastDowntime, uptimeRatio, downtimeCount);
        }
    }
}
