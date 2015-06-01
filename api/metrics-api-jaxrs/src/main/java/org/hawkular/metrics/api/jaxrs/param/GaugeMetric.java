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
import org.hawkular.metrics.core.api.GaugeDataPoint;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;

/**
 * @author jsanda
 */
public class GaugeMetric implements Metric<GaugeDataPoint> {

    private String tenantId;

    private MetricId id;

    private Map<String, String> tags = new HashMap<>();

    private List<GaugeDataPoint> dataPoints = new ArrayList<>();

    private Integer dataRetention;

    @JsonCreator
    public GaugeMetric(
            @JsonProperty("id") MetricId id,
            @JsonProperty("data") List<GaugeDataPoint> dataPoints) {
        this.id = id;
        this.dataPoints = dataPoints;
    }

    public GaugeMetric(MetricId id) {
        this.id = id;
    }

    @Override
    public MetricType getType() {
        return MetricType.GAUGE;
    }

    @Override
    public String getTenantId() {
        return tenantId;
    }

    public GaugeMetric setTenantId(String tenantId) {
        this.tenantId = tenantId;
        return this;
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
        return dataRetention;
    }

    public void setDataRetention(Integer dataRetention) {
        this.dataRetention = dataRetention;
    }

    @Override
    public List<GaugeDataPoint> getDataPoints() {
        return dataPoints;
    }

    public void addDataPoint(GaugeDataPoint dataPoint) {
        dataPoints.add(dataPoint);
    }
}
