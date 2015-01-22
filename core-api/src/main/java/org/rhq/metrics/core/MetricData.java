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
package org.rhq.metrics.core;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;

import org.rhq.metrics.util.TimeUUIDUtils;

/**
 * @author John Sanda
 */
public abstract class MetricData {


    public static final Comparator<MetricData> TIME_UUID_COMPARATOR = new Comparator<MetricData>() {
        @Override
        public int compare(MetricData d1, MetricData d2) {
            return TimeUUIDUtils.compare(d1.timeUUID, d2.timeUUID);
        }
    };

    protected UUID timeUUID;

    protected Metric metric;

    protected Set<Tag> tags = new HashSet<>();

    protected Long writeTime;

    /**
     * This is only used in calculating an updated TTL for data that has already been inserted. It might make more
     * sense to move this field closer to where the computations are performed and used.
     */
    protected Integer ttl;

    public MetricData(Metric metric, UUID timeUUID) {
        this.metric = metric;
        this.timeUUID = timeUUID;
    }

    public MetricData(Metric metric, UUID timeUUID, Set<Tag> tags) {
        this(metric, timeUUID, tags, null);
    }

    public MetricData(Metric metric, UUID timeUUID, Set<Tag> tags, Long writeTime) {
        this.metric = metric;
        this.timeUUID = timeUUID;
        this.tags = tags;
        this.writeTime = writeTime;
    }

    public MetricData(Metric metric, long timestamp) {
        this.metric = metric;
        this.timeUUID = TimeUUIDUtils.getTimeUUID(timestamp);
    }

    public MetricData(UUID timeUUID) {
        this.timeUUID = timeUUID;
    }

    public MetricData(UUID timeUUID, Set<Tag> tags) {
        this.timeUUID = timeUUID;
        this.tags = tags;
    }

    public MetricData(UUID timeUUID, Set<Tag> tags, Long writeTime) {
        this.timeUUID = timeUUID;
        this.tags = tags;
        this.writeTime = writeTime;
    }

    public MetricData(long timestamp) {
        timeUUID = TimeUUIDUtils.getTimeUUID(timestamp);
    }

    public Metric getMetric() {
        return metric;
    }

    public void setMetric(Metric metric) {
        this.metric = metric;
    }

    /**
     * The time based UUID for this data point
     */
    public UUID getTimeUUID() {
        return timeUUID;
    }

    /**
     * The UNIX timestamp of the {@link #getTimeUUID() timeUUID}
     */
    public long getTimestamp() {
        return UUIDs.unixTimestamp(timeUUID);
    }

    public Set<Tag> getTags() {
        return tags;
    }

    public void setTags(Set<Tag> tags) {
        this.tags = tags;
    }

    public Long getWriteTime() {
        return writeTime;
    }

    public void setWriteTime(Long writeTime) {
        this.writeTime = writeTime;
    }

    public Integer getTTL() {
        return ttl;
    }

    public void setTTL(Integer ttl) {
        this.ttl = ttl;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetricData)) return false;

        MetricData that = (MetricData) o;

        if (!timeUUID.equals(that.timeUUID)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return timeUUID.hashCode();
    }

}
