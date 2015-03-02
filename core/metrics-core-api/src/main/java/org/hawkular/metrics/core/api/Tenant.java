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
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Objects;

/**
 * Hawkular Metrics provides multi-tenancy support. This means that all data is implicitly partitioned by tenant. Tags,
 * data retention, and pre-computed aggregates are also per-tenant. Note that data retention and pre-computed aggregates
 * can be configured more narrowly, by tag and by individual metric.
 *
 * @author John Sanda
 */
public class Tenant {

    private String id;

    @JsonInclude(Include.NON_EMPTY)
    private List<AggregationTemplate> aggregationTemplates = new ArrayList<>();

    @JsonIgnore
    private RetentionSettings retentionSettings = new RetentionSettings();

    public String getId() {
        return id;
    }

    public Tenant setId(String id) {
        this.id = id;
        return this;
    }

    /**
     * The configured {@link org.hawkular.metrics.core.api.AggregationTemplate aggregation templates} for the tenant
     */
    public List<AggregationTemplate> getAggregationTemplates() {
        return aggregationTemplates;
    }

    public Tenant addAggregationTemplate(AggregationTemplate template) {
        aggregationTemplates.add(template);
        return this;
    }

    /**
     * The {@link org.hawkular.metrics.core.api.RetentionSettings data retention settings} for both
     * raw and aggregated data of all metric types
     */
    public RetentionSettings getRetentionSettings() {
        return retentionSettings;
    }

    public Tenant setRetention(MetricType type, int hours) {
        retentionSettings.put(type, hours);
        return this;
    }

    public Tenant setRetention(MetricType type, Interval interval, int hours) {
        retentionSettings.put(type, interval, hours);
        return this;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tenant tenant = (Tenant) o;

        if (!id.equals(tenant.id)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(getClass())
            .add("id", id)
            .add("aggregationTemplates", aggregationTemplates)
            .add("retentionSettings", retentionSettings)
            .toString();
    }
}
