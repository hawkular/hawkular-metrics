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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.annotation.JsonIgnore;

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

    @JsonIgnore
    protected UUID timeUUID;

    protected Map<String, String> tags = new HashMap<>();

    protected Long writeTime;

    /**
     * This is only used in calculating an updated TTL for data that has already been inserted. It might make more
     * sense to move this field closer to where the computations are performed and used.
     */
    protected Integer ttl;

    public MetricData(UUID timeUUID) {
        this.timeUUID = timeUUID;
    }

    public MetricData(UUID timeUUID, Map<String, String> tags) {
        this(timeUUID, tags, null);
    }

    public MetricData(UUID timeUUID, Map<String, String> tags, Long writeTime) {
        this.timeUUID = timeUUID;
        this.tags = tags;
        this.writeTime = writeTime;
    }

    public MetricData(long timestamp) {
        this.timeUUID = TimeUUIDUtils.getTimeUUID(timestamp);
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

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
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
