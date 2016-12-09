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

package org.hawkular.metrics.core.service.metrics;

import static java.util.Arrays.asList;

import static org.hawkular.metrics.model.MetricType.AVAILABILITY;
import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import rx.Observable;

/**
 *
 * @author Stefan Negrea
 */
public class JsonTagsITest extends BaseMetricsITest {

    @Test
    public void tagValueSearch() throws Exception {
        String tenantId = "jsonT1Tag";

        createTagMetrics(tenantId);

        //JSON PATH queries
        List<Metric<Double>> gauges = metricsService
                .findMetricsWithFilters(tenantId, GAUGE, ImmutableMap.of("a1", "json:$.foo"))
                .toSortedList((a, b) -> {
                    return a.getId().compareTo(b.getId());
                })
                .toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 3);
        assertMetricListById(gauges, "m1", "m2", "m3");

        gauges = metricsService
                .findMetricsWithFilters(tenantId, GAUGE, ImmutableMap.of("a1", "json:$..foo"))
                .toSortedList((a, b) -> {
                    return a.getId().compareTo(b.getId());
                })
                .toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 3);
        assertMetricListById(gauges, "m1", "m2", "m3");

        gauges = metricsService
                .findMetricsWithFilters(tenantId, GAUGE, ImmutableMap.of("a1", "json:$[?(@.foo == \"2\")]"))
                .toList()
                .toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 1);
        assertEquals(gauges.get(0).getId(), "m2");

        gauges = metricsService
                .findMetricsWithFilters(tenantId, GAUGE, ImmutableMap.of("a1", "json:$[?(@.foo == 1)]"))
                .toList()
                .toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 1);
        assertEquals(gauges.get(0).getId(), "m1");

        gauges = metricsService
                .findMetricsWithFilters(tenantId, GAUGE, ImmutableMap.of("a1", "json:$.foo.bar"))
                .toList()
                .toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 1);
        assertEquals(gauges.get(0).getId(), "m3");

        gauges = metricsService
                .findMetricsWithFilters(tenantId, GAUGE, ImmutableMap.of("a1", "json:$.foo.bar.foo"))
                .toList()
                .toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 0);

        // Request both regex and JSON Path matches
        gauges = metricsService
                .findMetricsWithFilters(tenantId, GAUGE, ImmutableMap.of("a1", "json:$.foo", "a2", "3"))
                .toList()
                .toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 4);
        assertMetricListById(gauges, "m1", "m2", "m3", "m4");

        gauges = metricsService
                .findMetricsWithFilters(tenantId, GAUGE, ImmutableMap.of("a2", "3", "a1", "json:$.foo"))
                .toList()
                .toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 4);
        assertMetricListById(gauges, "m1", "m2", "m3", "m4");

        gauges = metricsService
                .findMetricsWithFilters(tenantId, GAUGE, ImmutableMap.of("a1", "json:$.bar.foo", "a2", "3"))
                .toList()
                .toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 1);
        assertMetricListById(gauges, "m4");

        gauges = metricsService
                .findMetricsWithFilters(tenantId, GAUGE, ImmutableMap.of("a1", "json:$.bar"))
                .toList()
                .toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 0);

        gauges = metricsService
                .findMetricsWithFilters(tenantId, GAUGE, ImmutableMap.of("a1", "json:$..bar"))
                .toList()
                .toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 1);
        assertMetricListById(gauges, "m3");

        List<Metric<AvailabilityType>> availability = metricsService
                .findMetricsWithFilters(tenantId, AVAILABILITY, ImmutableMap.of("a1", "json:$.foo"))
                .toList()
                .toBlocking().lastOrDefault(null);
        assertEquals(availability.size(), 1);
        assertEquals(availability.get(0).getId(), "a1");

        List<Metric<Long>> counters = metricsService
                .findMetricsWithFilters(tenantId, COUNTER, ImmutableMap.of("a1", "json:$..foo"))
                .toList()
                .toBlocking().lastOrDefault(null);
        assertEquals(counters.size(), 1);
        assertEquals(counters.get(0).getId(), "c1");
    }

    private <T> void assertMetricListById(List<Metric<T>> actualMetrics, String... expectedMetricIds) {
        for (String expectedMetricId : expectedMetricIds) {
            boolean found = false;
            for (Metric<T> actualMetric : actualMetrics) {
                if (actualMetric.getId().equals(expectedMetricId)) {
                    found = true;
                    break;
                }
            }
            assertTrue(found, "Metric " + expectedMetricId + " was not found in the list of returned metrics.");
        }
    }

    protected Map<String, Metric<?>> createTagMetrics(String tenantId) throws Exception {
        ImmutableList<MetricId<?>> ids = ImmutableList.of(
                new MetricId<>(tenantId, GAUGE, "m1"),
                new MetricId<>(tenantId, GAUGE, "m2"),
                new MetricId<>(tenantId, GAUGE, "m3"),
                new MetricId<>(tenantId, GAUGE, "m4"),
                new MetricId<>(tenantId, GAUGE, "m5"),
                new MetricId<>(tenantId, GAUGE, "m6"),
                new MetricId<>(tenantId, GAUGE, "mA"),
                new MetricId<>(tenantId, GAUGE, "mB"),
                new MetricId<>(tenantId, GAUGE, "mC"),
                new MetricId<>(tenantId, GAUGE, "mD"),
                new MetricId<>(tenantId, GAUGE, "mE"),
                new MetricId<>(tenantId, GAUGE, "mF"),
                new MetricId<>(tenantId, GAUGE, "mG"),
                new MetricId<>(tenantId, AVAILABILITY, "a1"),
                new MetricId<>(tenantId, COUNTER, "c1"));

        @SuppressWarnings("unchecked")
        ImmutableList<ImmutableMap<String, String>> maps = ImmutableList.of(
                ImmutableMap.of("a1", "{\"foo\":1}", "a2", "1"),
                ImmutableMap.of("a1", "{\"foo\":2}"),
                ImmutableMap.of("a1", "{\"foo\": {\"bar\":3}}"),
                ImmutableMap.of("a1", "2", "a2", "3"),
                ImmutableMap.of("a1", "2", "a2", "4"),
                ImmutableMap.of("a2", "4"),
                ImmutableMap.of("hostname", "webfin01"),
                ImmutableMap.of("hostname", "webswe02"),
                ImmutableMap.of("hostname", "backendfin01"),
                ImmutableMap.of("hostname", "backendswe02"),
                ImmutableMap.of("owner", "hede"),
                ImmutableMap.of("owner", "hades"),
                ImmutableMap.of("owner", "had"),
                ImmutableMap.of("a1", "{\"foo\": 3}"),
                ImmutableMap.of("a1", "{\"foo\": 5}"));
        assertEquals(ids.size(), maps.size(), "ids' size should equal to maps' size");

        // Create the metrics
        List<Metric<?>> metricsToAdd = new ArrayList<>(ids.size());
        for (int i = 0; i < ids.size(); i++) {
            if(ids.get(i).getType() == GAUGE) {
                metricsToAdd.add(new Metric<>((MetricId<Double>) ids.get(i), maps.get(i), 24, asList(
                        new DataPoint<>(System.currentTimeMillis(), 1.0))));
            } else {
                metricsToAdd.add(new Metric<>(ids.get(i), maps.get(i), 24));
            }
        }

        // Insert metrics
        Observable.from(metricsToAdd)
                .subscribe(m -> metricsService.createMetric(m, false).toBlocking().lastOrDefault(null));

        return Observable.from(metricsToAdd).toMap(e -> e.getId()).toBlocking().lastOrDefault(null);
    }
}
