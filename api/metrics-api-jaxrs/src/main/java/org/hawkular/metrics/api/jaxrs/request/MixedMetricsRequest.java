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
package org.hawkular.metrics.api.jaxrs.request;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.wordnik.swagger.annotations.ApiModel;
import org.hawkular.metrics.api.jaxrs.model.Availability;
import org.hawkular.metrics.api.jaxrs.model.Counter;
import org.hawkular.metrics.api.jaxrs.model.Gauge;

/**
 * @author Stefan Negrea
 */
@ApiModel
public class MixedMetricsRequest {

    @JsonProperty("gauges")
    private List<Gauge> gauges;

    @JsonProperty("availabilities")
    private List<Availability> availabilities;

    @JsonProperty("counters")
    private List<Counter> counters;

    public List<Gauge> getGauges() {
        return gauges;
    }

    public List<Availability> getAvailabilities() {
        return availabilities;
    }

    public List<Counter> getCounters() {
        return counters;
    }

    @Override
    public String toString() {
        return "MixedMetricsRequest{" +
                "gaugeMetrics=" + gauges +
                ", availabilityMetrics=" + availabilities +
                ", counters=" + counters +
                '}';
    }
}
