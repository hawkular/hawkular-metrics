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
package org.hawkular.containers;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * @author mwringe
 */
@RunWith(Arquillian.class)
public class TestMetrics extends BaseContainerTests{

    @Test
    public void testMetricsStarted() throws Exception {
        JsonNode json = getJSON("/hawkular/metrics/status");
        assertEquals("STARTED", json.get("MetricsService").asText());
    }

    @Test
    public void testWriteData() throws Exception {
        String metricId = "testWriteData";

        JsonNode json = getJSON("/hawkular/metrics/gauges/" + metricId + "/data");
        assertEquals(null, json);

        sendMetric(metricId, "101", 1);
        sendMetric(metricId, "201", 2);

        json = getJSON("/hawkular/metrics/gauges/" + metricId + "/data");

        assertEquals("[{\"timestamp\":2,\"value\":201.0},{\"timestamp\":1," +
                             "\"value\":101.0}]", json.toString());
    }
}
