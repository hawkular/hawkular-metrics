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
package org.hawkular.metrics.core.impl;

import java.util.Objects;

import org.hawkular.metrics.core.api.DataPoint;

/**
 * @author jsanda
 */
public class TTLDataPoint<T extends DataPoint> {

    private T dataPoint;

    private int ttl;

    public TTLDataPoint(T dataPoint, int ttl) {
        this.dataPoint = dataPoint;
        this.ttl = ttl;
    }

    public T getDataPoint() {
        return dataPoint;
    }

    public int getTTL() {
        return ttl;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TTLDataPoint<?> that = (TTLDataPoint<?>) o;
        return Objects.equals(ttl, that.ttl) &&
                Objects.equals(dataPoint, that.dataPoint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataPoint, ttl);
    }

    @Override
    public String toString() {
        return com.google.common.base.Objects.toStringHelper(this)
                .add("dataPoint", dataPoint)
                .add("ttl", ttl)
                .toString();
    }
}
