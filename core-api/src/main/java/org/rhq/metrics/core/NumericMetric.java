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
import java.util.Set;
import java.util.UUID;

/**
 * @author John Sanda
 */
public class NumericMetric extends Metric<NumericData> {

    public NumericMetric(String tenantId, MetricId id) {
        super(tenantId, id);
    }

    public NumericMetric(String tenantId, MetricId id, Map<String, String> metadata) {
        super(tenantId, id, metadata);
    }

    public NumericMetric(String tenantId, MetricId id, Map<String, String> metadata, Integer dataRetention) {
        super(tenantId, id, metadata, dataRetention);
    }

    @Override
    public MetricType getType() {
        return MetricType.NUMERIC;
    }

    public void addData(long timestamp, double value) {
        addData(new NumericData(this, timestamp, value));
    }

    public void addData(UUID timeUUID, double value) {
        addData(new NumericData(this, timeUUID, value));
    }

    public void addData(UUID timeUUID, double value, Set<Tag> tags) {
        addData(new NumericData(this, timeUUID, value, tags));
    }

}
