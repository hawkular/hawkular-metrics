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
package org.hawkular.metrics.api.jaxrs.model;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonCreator.Mode;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.wordnik.swagger.annotations.ApiModel;

/**
 * @author Stefan Negrea
 */
@ApiModel
public class MixedMetricsRequest {
    private final List<Gauge> gauges;
    private final List<Availability> availabilities;
    private final List<Counter> counters;

    @JsonCreator(mode = Mode.PROPERTIES)
    @org.codehaus.jackson.annotate.JsonCreator
    @SuppressWarnings("unused")
    public MixedMetricsRequest(
            @JsonProperty("gauges")
            @org.codehaus.jackson.annotate.JsonProperty("gauges")
            List<Gauge> gauges,
            @JsonProperty("availabilities")
            @org.codehaus.jackson.annotate.JsonProperty("availabilities")
            List<Availability> availabilities,
            @JsonProperty("counters")
            @org.codehaus.jackson.annotate.JsonProperty("counters")
            List<Counter> counters
    ) {
        this.gauges = gauges == null ? emptyList() : unmodifiableList(gauges);
        this.availabilities = availabilities == null ? emptyList() : unmodifiableList(availabilities);
        this.counters = counters == null ? emptyList() : unmodifiableList(counters);
    }

    public List<Gauge> getGauges() {
        return gauges;
    }

    public List<Availability> getAvailabilities() {
        return availabilities;
    }

    public List<Counter> getCounters() {
        return counters;
    }

    /**
     * @return true if this instance has no data point (of any type)
     */
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
