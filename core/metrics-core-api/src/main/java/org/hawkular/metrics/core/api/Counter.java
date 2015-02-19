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

import com.google.common.base.Objects;

/**
 * A counter for tracking the number of some events, such as page hits.
 *
 * @author John Sanda
 */
public class Counter {

    private String tenantId;

    /** An identifier for grouping related counters. */
    private String group;

    /** The counter name. */
    private String name;

    /** The counter value. */
    private long value;

    public Counter(String tenantId, String group, String name, long value) {
        this.tenantId = tenantId;
        this.group = group;
        this.name = name;
        this.value = value;
    }

    /**
     * @return The id of the owning tenant
     */
    public String getTenantId() {
        return tenantId;
    }

    /**
     * @return An identifier for grouping related counters
     */
    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    /**
     * @return The counter name
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return The counter value
     */
    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Counter counter = (Counter) o;

        if (value != counter.value) return false;
        if (!group.equals(counter.group)) return false;
        if (!name.equals(counter.name)) return false;
        if (!tenantId.equals(counter.tenantId)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = tenantId.hashCode();
        result = 31 * result + group.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + (int) (value ^ (value >>> 32));
        return result;
    }

    /** @see java.lang.Object#toString() */
    @Override
    public String toString() {
        return Objects.toStringHelper(getClass().getSimpleName())
            .add("tenantId", tenantId)
            .add("group", group)
            .add("name", name)
            .add("value", value)
            .toString();
    }
}
