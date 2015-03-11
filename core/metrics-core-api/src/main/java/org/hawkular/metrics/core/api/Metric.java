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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Objects;

/**
 * @author John Sanda
 */
public abstract class Metric<T extends MetricData> {

    public static final long DPART = 0;

    private String tenantId;

    private MetricId id;

    private Map<String, String> tags = new HashMap<>();

    // When we implement date partitioning, dpart will have to be determined based on the
    // start and end params of queries. And it is possible the the date range spans
    // multiple date partitions.
    @JsonIgnore
    private long dpart = DPART;

    private List<T> data = new ArrayList<>();

    private Integer dataRetention;

    protected Metric(String tenantId, MetricId id) {
        this.tenantId = tenantId;
        this.id = id;
    }

    protected Metric(String tenantId, MetricId id, Map<String, String> tags) {
        this.tenantId = tenantId;
        this.id = id;
        this.tags = tags;
    }

    protected Metric(String tenantId, MetricId id, Map<String, String> tags, Integer dataRetention) {
        this.tenantId = tenantId;
        this.id = id;
        this.tags = tags;
        // If the data_retention column is not set, the driver returns zero instead of null.
        // We are (at least for now) using null to indicate that the metric does not have
        // the data retention set.
        if (dataRetention == null || dataRetention == 0) {
            this.dataRetention = null;
        } else {
            this.dataRetention = dataRetention;
        }
    }

    @JsonIgnore
    public abstract MetricType getType();

    public String getTenantId() {
        return tenantId;
    }

    public Metric<T> setTenantId(String tenantId) {
        this.tenantId = tenantId;
        return this;
    }

    public MetricId getId() {
        return id;
    }

    public void setId(MetricId id) {
        this.id = id;
    }

    public long getDpart() {
        return dpart;
    }

    public void setDpart(long dpart) {
        this.dpart = dpart;
    }

    /**
     * A set of key/value pairs that are shared by all data points for the metric. A good example is units like KB/sec.
     */
    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public List<T> getData() {
        return data;
    }

    public void addData(T d) {
        this.data.add(d);
    }

    public Integer getDataRetention() {
        return dataRetention;
    }

    public void setDataRetention(Integer dataRetention) {
        this.dataRetention = dataRetention;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        @SuppressWarnings("rawtypes")
        Metric metric = (Metric) o;

        if (dpart != metric.dpart) return false;
        if (tags != null ? !tags.equals(metric.tags) : metric.tags != null) return false;
        if (dataRetention != null ? !dataRetention.equals(metric.dataRetention) : metric.dataRetention != null)
            return false;
        if (!id.equals(metric.id)) return false;
        if (!tenantId.equals(metric.tenantId)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = tenantId.hashCode();
        result = 31 * result + id.hashCode();
        result = 31 * result + (tags != null ? tags.hashCode() : 0);
        result = 31 * result + (int) (dpart ^ (dpart >>> 32));
        result = 31 * result + (dataRetention != null ? dataRetention.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("tenantId", tenantId)
            .add("id", id)
            .add("tags", tags)
            .add("dpart", dpart)
            .add("dataRetention", dataRetention)
            .toString();
    }
}
