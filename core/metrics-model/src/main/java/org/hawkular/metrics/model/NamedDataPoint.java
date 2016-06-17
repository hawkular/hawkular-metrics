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

import java.util.Map;
import java.util.Objects;

import com.google.common.base.MoreObjects;

/**
 * @author jsanda
 */
public class NamedDataPoint<T> extends DataPoint<T> {

    private String name;

    public NamedDataPoint(String name, Long timestamp, T value, Map<String, String> tags) {
        super(timestamp, value, tags);
        this.name = name;
    }

    public NamedDataPoint(String name, DataPoint<T> dataPoint) {
        super(dataPoint.getTimestamp(), dataPoint.getValue(), dataPoint.getTags());
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        NamedDataPoint<?> that = (NamedDataPoint<?>) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), name);
    }

    @Override public String toString() {
        return MoreObjects.toStringHelper(this).
                add("name", name)
                .add("timestamp", getTimestamp())
                .add("value", getValue())
                .add("tags", getTags())
                .toString();
    }
}
