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

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author John Sanda
 */
public class AvailabilityMetric extends Metric<Availability> {

    @JsonCreator
    public AvailabilityMetric(@JsonProperty("id") MetricId id) {
        super("", id);
    }

    public AvailabilityMetric(String tenantId, MetricId id) {
        super(tenantId, id);
    }

    public AvailabilityMetric(String tenantId, MetricId id, Map<String, String> tags) {
        super(tenantId, id, tags);
    }

    public AvailabilityMetric(String tenantId, MetricId id, Map<String, String> tags, Integer dataRetention) {
        super(tenantId, id, tags, dataRetention);
    }

    @Override
    public MetricType getType() {
        return MetricType.AVAILABILITY;
    }
}
