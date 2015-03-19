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
package org.hawkular.metrics.core.api;

import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;

import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

/**
 * Statistics for availability data in a time range.
 *
 * @author Thomas Segismont
 */
@ApiModel(value = "Statistics for availability data in a time range.")
public class AvailabilityBucketDataPoint {
    private long start;
    private long end;
    private long downtimeDuration;
    private long lastDowntime;
    private double uptimeRatio;
    private long downtimeCount;

    public AvailabilityBucketDataPoint(
            long start,
            long end,
            long downtimeDuration,
            long lastDowntime,
            double uptimeRatio,
            long downtimeCount
    ) {
        this.start = start;
        this.end = end;
        this.downtimeDuration = downtimeDuration;
        this.lastDowntime = lastDowntime;
        this.uptimeRatio = uptimeRatio;
        this.downtimeCount = downtimeCount;
    }

    @ApiModelProperty(value = "Start timestamp of this bucket in milliseconds since epoch")
    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    @ApiModelProperty(value = "End timestamp of this bucket in milliseconds since epoch")
    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    @ApiModelProperty(value = "Total downtime duration in milliseconds")
    public long getDowntimeDuration() {
        return downtimeDuration;
    }

    public void setDowntimeDuration(long downtimeDuration) {
        this.downtimeDuration = downtimeDuration;
    }

    @ApiModelProperty(value = "Time of the last downtime in milliseconds since epoch")
    public long getLastDowntime() {
        return lastDowntime;
    }

    public void setLastDowntime(long lastDowntime) {
        this.lastDowntime = lastDowntime;
    }

    @ApiModelProperty(value = "Ratio of uptime to the length of the bucket")
    public double getUptimeRatio() {
        return uptimeRatio;
    }

    public void setUptimeRatio(double uptimeRatio) {
        this.uptimeRatio = uptimeRatio;
    }

    @ApiModelProperty(value = "Number of downtime periods in the bucket")
    public long getDowntimeCount() {
        return downtimeCount;
    }

    public void setDowntimeCount(long downtimeCount) {
        this.downtimeCount = downtimeCount;
    }

    public boolean isEmpty() {
        return isNaN(uptimeRatio);
    }

    @Override
    public String toString() {
        return "AvailabilityBucketDataPoint[" +
               "start=" + start +
               ", end=" + end +
               ", downtimeDuration=" + downtimeDuration +
               ", lastDowntime=" + lastDowntime +
               ", uptimeRatio=" + uptimeRatio +
               ", downtimeCount=" + downtimeCount +
               ']';
    }

    /**
     * Create {@link AvailabilityBucketDataPoint} instances following the builder pattern.
     */
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
         * @param start the start timestamp of this bucket data point
         * @param end   the end timestamp of this bucket data point
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

        public AvailabilityBucketDataPoint build() {
            return new AvailabilityBucketDataPoint(
                    start,
                    end,
                    downtimeDuration,
                    lastDowntime,
                    uptimeRatio,
                    downtimeCount
            );
        }
    }
}
