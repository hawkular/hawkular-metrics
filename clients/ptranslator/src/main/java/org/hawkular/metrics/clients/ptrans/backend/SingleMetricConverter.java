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

package org.hawkular.metrics.clients.ptrans.backend;

import org.hawkular.metrics.client.common.SingleMetric;

import io.vertx.core.json.JsonObject;

/**
 * @author Thomas Segismont
 */
public class SingleMetricConverter {

    public static JsonObject toJsonObject(SingleMetric singleMetric) {
        JsonObject jsonObject = new JsonObject().put("id", singleMetric.getSource())
                .put("timestamp", singleMetric.getTimestamp());
        switch (singleMetric.getMetricType()) {
            case COUNTER:
                jsonObject.put("type", "counter")
                        .put("value", singleMetric.getValue().longValue());
            default:
                // Process anything else as a gauge
                jsonObject.put("type", "gauge")
                        .put("value", singleMetric.getValue());
        }
        return jsonObject;
    }

    private SingleMetricConverter() {
        // Utility class
    }
}
