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
package org.hawkular.metrics.model;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonCreator.Mode;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import io.swagger.annotations.ApiModel;

/**
 * @author Stefan Negrea
 */
@ApiModel(description = "Data points to store, grouped by metric type")
public class MixedMetricsRequest {
    private final List<Metric<Double>> gauges;
    private final List<Metric<AvailabilityType>> availabilities;
    private final List<Metric<Long>> counters;
    private final List<Metric<String>> strings;

    @JsonCreator(mode = Mode.PROPERTIES)
    public MixedMetricsRequest(
            @JsonProperty("gauges")
            List<Metric<Double>> gauges,
            @JsonProperty("availabilities")
            List<Metric<AvailabilityType>> availabilities,
            @JsonProperty("counters")
            List<Metric<Long>> counters,
            @JsonProperty("strings")
            List<Metric<String>> strings
    ) {
        this.gauges = gauges == null ? emptyList() : unmodifiableList(gauges);
        this.availabilities = availabilities == null ? emptyList() : unmodifiableList(availabilities);
        this.counters = counters == null ? emptyList() : unmodifiableList(counters);
        this.strings = strings == null ? emptyList() : unmodifiableList(strings);
    }

    public List<Metric<Double>> getGauges() {
        return gauges;
    }

    public List<Metric<AvailabilityType>> getAvailabilities() {
        return availabilities;
    }

    public List<Metric<Long>> getCounters() {
        return counters;
    }

    public List<Metric<String>> getStrings() {
        return strings;
    }

    /**
     * @return true if this instance has no data point (of any type)
     */
    @JsonIgnore
    public boolean isEmpty() {
        return gauges.isEmpty() && availabilities.isEmpty() && counters.isEmpty() && strings.isEmpty();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("gauges", gauges)
                .add("availabilities", availabilities)
                .add("counters", counters)
                .add("strings", strings)
                .omitNullValues()
                .toString();
    }
}
