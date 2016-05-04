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
package org.hawkular.metrics.model;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

/**
 * Hawkular Metrics provides multi-tenancy support. This means that all data is implicitly partitioned by tenant. Tags,
 * data retention, and pre-computed aggregates are also per-tenant. Note that data retention and pre-computed
 * aggregates
 * can be configured more narrowly, by tag and by individual metric.
 *
 * @author John Sanda
 */
public class Tenant {
    private final String id;
    private final Map<MetricType<?>, Integer> retentionSettings;

    public Tenant(String id) {
        this(id, null);
    }

    public Tenant(String id, Map<MetricType<?>, Integer> retentionSettings) {
        checkArgument(id != null, "Tenant id is null");
        this.id = id;
        this.retentionSettings = retentionSettings == null ? ImmutableMap.of() : ImmutableMap.copyOf(retentionSettings);
    }

    public String getId() {
        return id;
    }

    public Map<MetricType<?>, Integer> getRetentionSettings() {
        return retentionSettings;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Tenant tenant = (Tenant) o;
        return id.equals(tenant.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("retentionSettings", retentionSettings)
                .omitNullValues()
                .toString();
    }
}
