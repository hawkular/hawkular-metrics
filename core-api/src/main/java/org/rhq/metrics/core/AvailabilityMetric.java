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
package org.rhq.metrics.core;

import java.util.Map;
import java.util.UUID;

/**
 * @author John Sanda
 */
public class AvailabilityMetric extends Metric<Availability> {

    public AvailabilityMetric(String tenantId, MetricId id) {
        super(tenantId, id);
    }

    public AvailabilityMetric(String tenantId, MetricId id, Map<String, String> metadata) {
        super(tenantId, id, metadata);
    }

    public AvailabilityMetric(String tenantId, MetricId id, Map<String, String> metadata, Integer dataRetention) {
        super(tenantId, id, metadata, dataRetention);
    }

    @Override
    public MetricType getType() {
        return MetricType.AVAILABILITY;
    }

    public void addAvailability(long timestamp, AvailabilityType availability) {
        addData(new Availability(this, timestamp, availability));
    }

    public void addAvailability(long timestamp, String availability) {
        addData(new Availability(this, timestamp, AvailabilityType.fromString(availability)));
    }

    public void addAvailability(UUID timeUUID, AvailabilityType availability) {
        addData(new Availability(this, timeUUID, availability));
    }
}
