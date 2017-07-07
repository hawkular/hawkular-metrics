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
package org.hawkular.metrics.model;

import static java.lang.Double.NaN;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link BucketPoint} for availability metrics.
 *
 * @author Thomas Segismont
 * @author Jay Shaughnessy
 */
public final class AvailabilityBucketPoint extends BucketPoint {
    private final Map<AvailabilityType, Long> durationMap;
    private final Long lastNotUptime;
    private final Double uptimeRatio;
    private final Long notUpCount;
    private final Long samples;

    protected AvailabilityBucketPoint(long start, long end, Map<AvailabilityType, Long> durationMap,
            long lastNotUptime, double uptimeRatio, long notUpCount, long samples) {
        super(start, end);
        this.durationMap = durationMap;
        this.lastNotUptime = lastNotUptime;
        this.uptimeRatio = getDoubleValue(uptimeRatio);
        this.notUpCount = notUpCount;
        this.samples = samples;
    }

    /**
     * @return The number of segments of where the availability type is not UP. A segment can combine multiple
     * NotUP statuses (e.g. a change from DOWN to UNKNOWN does not increment the count).
     */
    public Long getNotUpCount() {
        if (isEmpty()) {
            return null;
        }

        return notUpCount;
    }

    public Map<AvailabilityType, Long> getDurationMap() {
        return durationMap;
    }

    public Long getAdminDuration() {
        if (isEmpty()) {
            return null;
        }
        return durationMap.getOrDefault(AvailabilityType.ADMIN, 0L);
    }

    public Long getDownDuration() {
        if (isEmpty()) {
            return null;
        }
        return durationMap.getOrDefault(AvailabilityType.DOWN, 0L);
    }

    public Long getUnknownDuration() {
        if (isEmpty()) {
            return null;
        }
        return durationMap.getOrDefault(AvailabilityType.UNKNOWN, 0L);
    }

    public Long getUpDuration() {
        if (isEmpty()) {
            return null;
        }
        return durationMap.getOrDefault(AvailabilityType.UP, 0L);
    }

    public Long getNotUpDuration() {
        if (isEmpty()) {
            return null;
        }
        return getAdminDuration() + getDownDuration() + getUnknownDuration();
    }

    public Long getLastNotUptime() {
        if (isEmpty()) {
            return null;
        }
        return lastNotUptime;
    }

    public Double getUptimeRatio() {
        return uptimeRatio;
    }

    public Long getSamples() {
        return samples;
    }

    /**
     * @return Convenience method to return number of up segments <code>(samples - notUpCount)</code>
     */
    public Long getUpCount() {
        return samples - notUpCount;
    }

    @Override
    public boolean isEmpty() {
        return uptimeRatio == null;
    }

    @Override
    public String toString() {
        return "AvailabilityBucketPoint [durationMap=" + durationMap + ", lastNotUptime=" + lastNotUptime
                + ", uptimeRatio=" + uptimeRatio + ", notUpCount=" + notUpCount + ", samples=" + samples + "]";
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
        private Map<AvailabilityType, Long> durationMap = new HashMap<>();
        private long lastNotUptime = 0;
        private double uptimeRatio = NaN;
        private long notUpCount = 0;
        private long samples = 0;

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

        public Builder setDurationMap(Map<AvailabilityType, Long> durationMap) {
            this.durationMap.putAll(durationMap);
            return this;
        }

        public Builder setAdminDuration(long adminDuration) {
            this.durationMap.put(AvailabilityType.ADMIN, adminDuration);
            return this;
        }

        public Builder setDownDuration(long downDuration) {
            this.durationMap.put(AvailabilityType.DOWN, downDuration);
            return this;
        }

        public Builder setUnknownDuration(long unknownDuration) {
            this.durationMap.put(AvailabilityType.UNKNOWN, unknownDuration);
            return this;
        }

        public Builder setUpDuration(long upDuration) {
            this.durationMap.put(AvailabilityType.UP, upDuration);
            return this;
        }

        public Builder setLastNotUptime(long lastNotUptime) {
            this.lastNotUptime = lastNotUptime;
            return this;
        }

        public Builder setUptimeRatio(double uptimeRatio) {
            this.uptimeRatio = uptimeRatio;
            return this;
        }

        public Builder setNotUptimeCount(long notUptimeCount) {
            this.notUpCount = notUptimeCount;
            return this;
        }

        public Builder setSamples(long samples) {
            this.samples = samples;
            return this;
        }

        public AvailabilityBucketPoint build() {
            return new AvailabilityBucketPoint(start, end, durationMap, lastNotUptime, uptimeRatio, notUpCount,
                    samples);
        }
    }
}
