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

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Objects;

/**
 * @author John Sanda
 */
public class MetricId {

    private String name;

    private Interval interval;

    public MetricId(String name) {
        this(name, Interval.NONE);
    }

    public MetricId(String name, Interval interval) {
        this.name = name;
        this.interval = interval;
    }

    @JsonValue
    public String getName() {
        return name;
    }

    public Interval getInterval() {
        return interval;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MetricId metricId = (MetricId) o;

        if (!interval.equals(metricId.interval)) return false;
        if (!name.equals(metricId.name)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + interval.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("name", name).add("interval", interval).toString();
    }
}
