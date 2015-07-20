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
package org.hawkular.metrics.core.impl.tags;

import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;

/**
 * Represents required information from MetricIndex to transform later on to Metric.
 *
 * HWKMETRICS-114 might render this obsolete
 *
 * @author Michael Burman
 */
public class MetricIndex {
    private final MetricType type;
    private final MetricId id;

    public MetricIndex(MetricType type, MetricId id) {
        this.type = type;
        this.id = id;
    }

    public MetricType getType() {
        return type;
    }

    public MetricId getId() {
        return id;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof MetricIndex) {
            MetricIndex i = (MetricIndex) obj;
            return i.getId().getName().equals(this.getId().getName())
                    && i.getType().equals(this.getType());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.getId().getName().hashCode() + this.getType().hashCode();
    }
}
