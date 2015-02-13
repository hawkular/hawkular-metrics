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
 * @author John Sanda
 */
public class Retention {

    private MetricId id;

    private int value;

    public Retention(MetricId id, int value) {
        this.id = id;
        this.value = value;
    }

    public MetricId getId() {
        return id;
    }

    public int getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Retention retention = (Retention) o;

        if (value != retention.value) return false;
        if (!id.equals(retention.id)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + value;
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("id", id).add("value", value).toString();
    }
}
