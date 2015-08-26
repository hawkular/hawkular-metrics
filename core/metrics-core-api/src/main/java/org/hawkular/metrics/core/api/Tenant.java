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

import java.util.Collections;
import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

/**
 * Hawkular Metrics provides multi-tenancy support. This means that all data is implicitly partitioned by tenant. Tags,
 * data retention, and pre-computed aggregates are also per-tenant. Note that data retention and pre-computed aggregates
 * can be configured more narrowly, by tag and by individual metric.
 *
 * @author John Sanda
 */
public class Tenant {

    private String id;

    private Map<MetricType<?>, Integer> retentionSettings = Collections.emptyMap();

    public Tenant(String id) {
        this.id = id;
    }

    public Tenant(String id, Map<MetricType<?>, Integer> retentionSettings) {
        this.id = id;
        this.retentionSettings = ImmutableMap.copyOf(retentionSettings);
    }

    public String getId() {
        return id;
    }

    public Map<MetricType<?>, Integer> getRetentionSettings() {
        return retentionSettings;
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
            .add("retentionSettings", retentionSettings)
            .toString();
    }
}
