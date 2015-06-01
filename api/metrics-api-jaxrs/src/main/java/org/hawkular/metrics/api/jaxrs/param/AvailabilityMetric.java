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
package org.hawkular.metrics.api.jaxrs.param;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.hawkular.metrics.core.api.AvailabilityDataPoint;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;

/**
 * @author jsanda
 */
public class AvailabilityMetric implements Metric<AvailabilityDataPoint> {

    private String tenantId;

    private MetricId id;

    private Map<String, String> tags = new HashMap<>();

    private List<AvailabilityDataPoint> dataPoints = new ArrayList<>();

    private Integer dataRetention;

    @JsonCreator
    public AvailabilityMetric(
            @JsonProperty("id") MetricId id,
            @JsonProperty("data") List<AvailabilityDataPoint> dataPoints) {
        this.id = id;
        this.dataPoints = dataPoints;
    }

    public AvailabilityMetric(String tenantId, MetricId id) {
        this.tenantId = tenantId;
        this.id = id;
    }

    @Override
    public MetricType getType() {
        return MetricType.AVAILABILITY;
    }

    @Override
    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public MetricId getId() {
        return id;
    }

    @Override
    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public Integer getDataRetention() {
        return null;
    }

    public void setDataRetention(Integer dataRetention) {
        this.dataRetention = dataRetention;
    }

    @Override
    public List<AvailabilityDataPoint> getDataPoints() {
        return dataPoints;
    }
}
