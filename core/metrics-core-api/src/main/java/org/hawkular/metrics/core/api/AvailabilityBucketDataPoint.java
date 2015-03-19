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
    private long timestamp;
    private long downtimeDuration;
    private long lastDowntime;
    private double uptimeRatio;
    private long downtimeCount;

    public static AvailabilityBucketDataPoint newEmptyInstance(long timestamp) {
        AvailabilityBucketDataPoint dataPoint = new AvailabilityBucketDataPoint();
        dataPoint.setTimestamp(timestamp);
        dataPoint.setUptimeRatio(NaN);
        return dataPoint;
    }

    @ApiModelProperty(value = "Time when the value was obtained in milliseconds since epoch")
    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
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
               "timestamp=" + timestamp +
               ", downtimeDuration=" + downtimeDuration +
               ", lastDowntime=" + lastDowntime +
               ", upRatio=" + uptimeRatio +
               ", downtimeCount=" + downtimeCount +
               ']';
    }
}
