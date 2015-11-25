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

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonCreator.Mode;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import io.swagger.annotations.ApiModel;

/**
 * @author Stefan Negrea
 */
@ApiModel(description = "Data points to store, grouped by metric type")
public class MixedMetricsRequest {
    private final List<MetricDefinition<Double>> gauges;
    private final List<MetricDefinition<AvailabilityType>> availabilities;
    private final List<MetricDefinition<Long>> counters;

    @JsonCreator(mode = Mode.PROPERTIES)
    @org.codehaus.jackson.annotate.JsonCreator
    public MixedMetricsRequest(
            @JsonProperty("gauges")
            @org.codehaus.jackson.annotate.JsonProperty("gauges")
            List<MetricDefinition<Double>> gauges,
            @JsonProperty("availabilities")
            @org.codehaus.jackson.annotate.JsonProperty("availabilities")
            List<MetricDefinition<AvailabilityType>> availabilities,
            @JsonProperty("counters")
            @org.codehaus.jackson.annotate.JsonProperty("counters")
            List<MetricDefinition<Long>> counters
    ) {
        this.gauges = gauges == null ? emptyList() : unmodifiableList(gauges);
        this.availabilities = availabilities == null ? emptyList() : unmodifiableList(availabilities);
        this.counters = counters == null ? emptyList() : unmodifiableList(counters);
    }

    public List<MetricDefinition<Double>> getGauges() {
        return gauges;
    }

    public List<MetricDefinition<AvailabilityType>> getAvailabilities() {
        return availabilities;
    }

    public List<MetricDefinition<Long>> getCounters() {
        return counters;
    }

    /**
     * @return true if this instance has no data point (of any type)
     */
    @JsonIgnore
    @org.codehaus.jackson.annotate.JsonIgnore
    public boolean isEmpty() {
        return gauges.isEmpty() && availabilities.isEmpty() && counters.isEmpty();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("gauges", gauges)
                .add("availabilities", availabilities)
                .add("counters", counters)
                .omitNullValues()
                .toString();
    }
}
