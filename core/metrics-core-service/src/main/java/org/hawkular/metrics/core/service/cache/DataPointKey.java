/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.service.cache;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

import org.infinispan.distribution.group.Group;

/**
 * @author jsanda
 */
public class DataPointKey implements Serializable {

    private static final long serialVersionUID = 5285838073841938813L;

    private byte[] id;

//    private String metric;
//
//    private long timestamp;
//
    private String timeSlice;

    DataPointKey() {
    }

//    public DataPointKey(String tenantId, String metric, long timestamp, long timeSlice) {
//        this.tenantId = tenantId;
//        this.metric = metric;
//        this.timestamp = timestamp;
//        this.timeSlice = Long.toString(timeSlice);
//    }

    public DataPointKey(byte[] id, String timeSlice) {
        this.id = id;
        this.timeSlice = timeSlice;
    }

//    public String getTenantId() {
//        return tenantId;
//    }
//
//    public String getMetric() {
//        return metric;
//    }
//
//    public long getTimestamp() {
//        return timestamp;
//    }

    @Group
    public String getTimeSlice() {
        return timeSlice;
    }

//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//        DataPointKey that = (DataPointKey) o;
//        return timestamp == that.timestamp &&
//                Objects.equals(tenantId, that.tenantId) &&
//                Objects.equals(metric, that.metric) &&
//                Objects.equals(timeSlice, that.timeSlice);
//    }
//
//    @Override
//    public int hashCode() {
//        return Objects.hash(tenantId, metric, timestamp, timeSlice);
//    }
//
//    @Override
//    public String toString() {
//        return MoreObjects.toStringHelper(this)
//                .add("tenantId", tenantId)
//                .add("metric", metric)
//                .add("timestamp", timestamp)
//                .add("timeSlice", timeSlice)
//                .toString();
//    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataPointKey that = (DataPointKey) o;
        return Arrays.equals(id, that.id) &&
                Objects.equals(timeSlice, that.timeSlice);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, timeSlice);
    }
}