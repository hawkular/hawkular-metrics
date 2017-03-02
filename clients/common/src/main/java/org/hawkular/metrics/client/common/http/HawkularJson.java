/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.client.common.http;

import java.util.Map;
import java.util.function.BiFunction;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

/**
 * Some Json utility for Hawkular data model
 * @author Joel Takvorian
 */
public final class HawkularJson {

    private HawkularJson() {
    }

    public static String metricsToString(Long timestamp,
                                         Map<String, Long> counters,
                                         Map<String, Double> gauges) {
        JsonObjectBuilder builder = Json.createObjectBuilder();
        if (!counters.isEmpty()) {
            builder.add("counters", metricsJson(timestamp, counters, HawkularJson::longDataPoint));
        }
        if (!gauges.isEmpty()) {
            builder.add("gauges", metricsJson(timestamp, gauges, HawkularJson::doubleDataPoint));
        }
        return builder.build().toString();
    }

    public static String tagsToString(Map<String, String> tags) {
        JsonObjectBuilder jsonObjectBuilder = Json.createObjectBuilder();
        tags.forEach(jsonObjectBuilder::add);
        return jsonObjectBuilder.build().toString();
    }

    private static <T> JsonArray metricsJson(Long timestamp,
                                             Map<String, T> metricsPoints,
                                             BiFunction<Long, T, JsonObject> bf) {
        final JsonArrayBuilder builder = Json.createArrayBuilder();
        metricsPoints.entrySet().stream()
                .map(e -> metricJson(e.getKey(), bf.apply(timestamp, e.getValue())))
                .forEach(builder::add);
        return builder.build();
    }

    private static JsonObject metricJson(String name, JsonObject dataPoint) {
        return Json.createObjectBuilder()
                .add("id", name)
                .add("dataPoints", Json.createArrayBuilder().add(dataPoint).build())
                .build();
    }

    private static JsonObject doubleDataPoint(long timestamp, double value) {
        return Json.createObjectBuilder()
                .add("timestamp", timestamp)
                .add("value", value)
                .build();
    }

    private static JsonObject longDataPoint(long timestamp, long value) {
        return Json.createObjectBuilder()
                .add("timestamp", timestamp)
                .add("value", value)
                .build();
    }
}
