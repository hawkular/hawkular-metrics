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

import java.util.Objects;

import com.google.common.base.MoreObjects;

/**
 * @author jsanda
 */
public class RollupKey {

    private DataPointKey key;

    private int rollup;

    RollupKey() {
        super();
    }

    public RollupKey(DataPointKey key, int rollup) {
        this.key = key;
        this.rollup = rollup;
    }

    public DataPointKey getKey() {
        return key;
    }

    public int getRollup() {
        return rollup;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RollupKey rollupKey = (RollupKey) o;
        return rollup == rollupKey.rollup &&
                Objects.equals(key, rollupKey.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, rollup);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("key", key).add("rollup", rollup).toString();
    }
}
