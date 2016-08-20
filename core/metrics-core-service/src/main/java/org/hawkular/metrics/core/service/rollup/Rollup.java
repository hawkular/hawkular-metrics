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
package org.hawkular.metrics.core.service.rollup;

import java.util.Objects;

import com.google.common.base.MoreObjects;

/**
 * @author jsanda
 */
public class Rollup {

    private int resolution;

    private int defaultTTL;

    public Rollup(int resolution, int defaultTTL) {
        this.resolution = resolution;
        this.defaultTTL = defaultTTL;
    }

    public int getResolution() {
        return resolution;
    }

    public int getDefaultTTL() {
        return defaultTTL;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Rollup rollup = (Rollup) o;
        return resolution == rollup.resolution &&
                defaultTTL == rollup.defaultTTL;
    }

    @Override
    public int hashCode() {
        return Objects.hash(resolution, defaultTTL);
    }

    @Override public String toString() {
        return MoreObjects.toStringHelper(this).add("resolution", resolution).add("defaultTTL", defaultTTL).toString();
    }
}
