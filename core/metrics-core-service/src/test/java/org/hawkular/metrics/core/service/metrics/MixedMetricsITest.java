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
package org.hawkular.metrics.core.service.metrics;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MINUTES;

import static org.hawkular.metrics.core.service.Functions.makeSafe;
import static org.hawkular.metrics.model.MetricType.AVAILABILITY;
import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.hawkular.metrics.model.AvailabilityType;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.Retention;
import org.hawkular.metrics.model.Tenant;
import org.hawkular.metrics.model.exception.MetricAlreadyExistsException;
import org.hawkular.metrics.model.exception.TenantAlreadyExistsException;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import rx.Observable;

public class MixedMetricsITest extends BaseMetricsITest {

    @Test
    public void createTenants() throws Exception {
        String t1Id = createRandomTenantId();
        Tenant t1 = new Tenant(t1Id, ImmutableMap.of(GAUGE, 1, AVAILABILITY, 1));
        Tenant t2 = new Tenant(createRandomTenantId(), ImmutableMap.of(GAUGE, 7));
        Tenant t3 = new Tenant(createRandomTenantId(), ImmutableMap.of(AVAILABILITY, 2));
        Tenant t4 = new Tenant(createRandomTenantId());

        Observable.concat(
                metricsService.createTenant(t1, false),
                metricsService.createTenant(t2, false),
                metricsService.createTenant(t3, false),
                metricsService.createTenant(t4, false)).toBlocking().lastOrDefault(null);

        Set<Tenant> actualTenants = ImmutableSet.copyOf(metricsService.getTenants().toBlocking().toIterable());
        Set<Tenant> expectedTenants = ImmutableSet.of(t1, t2, t3, t4);

        for (Tenant expected : expectedTenants) {
            Tenant actual = null;
            for (Tenant t : actualTenants) {
                if (t.getId().equals(expected.getId())) {
                    actual = t;
                }
            }
            assertNotNull(actual, "Expected to find a tenant with id [" + expected.getId() + "]");
            assertEquals(actual, expected, "The tenant does not match");
        }

        assertDataRetentionsIndexContains(t1.getId(), GAUGE, ImmutableSet.of(new Retention(
                new MetricId<>(t1.getId(), GAUGE, makeSafe(GAUGE.getText())), 1)));
        assertDataRetentionsIndexContains(t1.getId(), AVAILABILITY, ImmutableSet.of(new Retention(
                new MetricId<>(t1.getId(), AVAILABILITY, makeSafe(AVAILABILITY.getText())), 1)));

        t1 = new Tenant(t1Id, ImmutableMap.of(GAUGE, 101, AVAILABILITY, 102));
        try {
            metricsService.createTenant(t1, false).toBlocking().lastOrDefault(null);
            fail("Tenant should already exist and not overwritten.");
        } catch (TenantAlreadyExistsException e) {

        }

        try {
            metricsService.createTenant(t1, true).toBlocking().lastOrDefault(null);
        } catch (MetricAlreadyExistsException e) {
            fail("Tenant should already exist and be overwritten.");
        }

        actualTenants = ImmutableSet.copyOf(metricsService.getTenants().toBlocking().toIterable());
        assertDataRetentionsIndexMatches(t1.getId(), GAUGE, ImmutableSet.of(new Retention(
                new MetricId<>(t1.getId(), GAUGE, makeSafe(GAUGE.getText())), 101)));
        assertDataRetentionsIndexMatches(t1.getId(), AVAILABILITY, ImmutableSet.of(new Retention(
                new MetricId<>(t1.getId(), AVAILABILITY, makeSafe(AVAILABILITY.getText())), 102)));
    }

    @Test
    public void createMetricsIdxTenants() throws Exception {
        String tenantId = createRandomTenantId();
        Metric<Double> em1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "em1"));
        metricsService.createMetric(em1, false).toBlocking().lastOrDefault(null);

        Metric<Double> actual = metricsService.<Double> findMetric(em1.getMetricId())
                .toBlocking()
                .lastOrDefault(null);
        assertNotNull(actual);
        Metric<Double> em2 = new Metric<>(em1.getMetricId(), 7);
        assertEquals(actual, em2, "The metric does not match the expected value");

        Set<Tenant> actualTenants = ImmutableSet.copyOf(metricsService.getTenants().toBlocking().toIterable());
        assertTrue(actualTenants.contains(new Tenant(tenantId)));

        Tenant t1 = new Tenant(tenantId, ImmutableMap.of(GAUGE, 1, AVAILABILITY, 1));
        metricsService.createTenant(t1, false).toBlocking().lastOrDefault(null);

        actualTenants = ImmutableSet.copyOf(metricsService.getTenants().toBlocking().toIterable());
        assertTrue(actualTenants.contains(t1));
    }

    @Test
    public void createAndFindMetrics() throws Exception {
        String tenantId = createRandomTenantId();
        Metric<Double> em1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "em1"));
        metricsService.createMetric(em1, false).toBlocking().lastOrDefault(null);
        Metric<Double> actual = metricsService.<Double> findMetric(em1.getMetricId())
                .toBlocking()
                .lastOrDefault(null);
        assertNotNull(actual);
        Metric<Double> em2 = new Metric<>(em1.getMetricId(), 7);
        assertEquals(actual, em2, "The metric does not match the expected value");

        Metric<Double> m1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m1"), ImmutableMap.of("a1", "1", "a2", "2"), 24);
        metricsService.createMetric(m1, false).toBlocking().lastOrDefault(null);

        actual = metricsService.<Double> findMetric(m1.getMetricId()).toBlocking().last();
        assertEquals(actual, m1, "The metric does not match the expected value");

        Metric<AvailabilityType> m2 = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, "m2"),
                ImmutableMap.of("a3", "3", "a4", "3"), DEFAULT_TTL);
        metricsService.createMetric(m2, false).toBlocking().lastOrDefault(null);

        // Find definitions with given tags
        Map<String, String> tagMap = new HashMap<>();
        tagMap.putAll(ImmutableMap.of("a1", "1", "a2", "2"));
        tagMap.putAll(ImmutableMap.of("a3", "3", "a4", "3"));

        // Test that distinct filtering does not remove same name from different types
        Metric<Double> gm2 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m2"),
                ImmutableMap.of("a3", "3", "a4", "3"), null);
        metricsService.createMetric(gm2, false).toBlocking().lastOrDefault(null);

        Metric<AvailabilityType> actualAvail = metricsService.<AvailabilityType> findMetric(m2.getMetricId())
                .toBlocking()
                .last();
        assertEquals(actualAvail, m2, "The metric does not match the expected value");

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
        metricsService.createMetric(m1, false).subscribe(
                nullArg -> {
                },
                t -> {
                    exceptionRef.set(t);
                    latch.countDown();
                },
                latch::countDown);
        latch.await(10, TimeUnit.SECONDS);
        assertTrue(exceptionRef.get() != null && exceptionRef.get() instanceof MetricAlreadyExistsException,
                "Expected a " + MetricAlreadyExistsException.class.getSimpleName() + " to be thrown");

        Metric<Double> m3 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m3"), emptyMap(), 24);
        metricsService.createMetric(m3, false).toBlocking().lastOrDefault(null);

        Metric<Double> m4 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m4"),
                ImmutableMap.of("a1", "A", "a2", ""), null);
        metricsService.createMetric(m4, false).toBlocking().lastOrDefault(null);

        assertMetricIndexMatches(tenantId, GAUGE, asList(new Metric<>(em1.getMetricId(), 7), m1,
                new Metric<>(gm2.getMetricId(), gm2.getTags(), 7),
                m3,
                new Metric<>(m4.getMetricId(), m4.getTags(), 7)));
        assertMetricIndexMatches(tenantId, AVAILABILITY, singletonList(m2));

        assertDataRetentionsIndexMatches(tenantId, GAUGE, ImmutableSet.of(new Retention(m3.getMetricId(), 24),
                new Retention(m1.getMetricId(), 24)));

        assertMetricsTagsIndexMatches(tenantId, "a1", asList(
                new MetricsTagsIndexEntry("1", m1.getMetricId()),
                new MetricsTagsIndexEntry("A", m4.getMetricId())));
    }

    @Test
    public void shouldReceiveInsertedDataNotifications() throws Exception {
        String tenantId = createRandomTenantId();
        ImmutableList<MetricType<?>> metricTypes = ImmutableList.of(GAUGE, COUNTER, AVAILABILITY);
        AvailabilityType[] availabilityTypes = AvailabilityType.values();

        int numberOfPoints = 10;

        List<Metric<?>> actual = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(metricTypes.size() * numberOfPoints);
        metricsService.insertedDataEvents()
                .filter(metric -> metric.getMetricId().getTenantId().equals(tenantId))
                .subscribe(metric -> {
                    actual.add(metric);
                    latch.countDown();
                });

        List<Metric<?>> expected = new ArrayList<>();
        for (MetricType<?> metricType : metricTypes) {
            for (int i = 0; i < numberOfPoints; i++) {
                if (metricType == GAUGE) {

                    DataPoint<Double> dataPoint = new DataPoint<>((long) i, (double) i);
                    MetricId<Double> metricId = new MetricId<>(tenantId, GAUGE, "gauge");
                    Metric<Double> metric = new Metric<>(metricId, ImmutableList.of(dataPoint));

                    metricsService.addDataPoints(GAUGE, Observable.just(metric)).subscribe();

                    expected.add(metric);

                } else if (metricType == COUNTER) {

                    DataPoint<Long> dataPoint = new DataPoint<>((long) i, (long) i);
                    MetricId<Long> metricId = new MetricId<>(tenantId, COUNTER, "counter");
                    Metric<Long> metric = new Metric<>(metricId, ImmutableList.of(dataPoint));

                    metricsService.addDataPoints(COUNTER, Observable.just(metric)).subscribe();

                    expected.add(metric);

                } else if (metricType == AVAILABILITY) {

                    AvailabilityType availabilityType = availabilityTypes[i % availabilityTypes.length];
                    DataPoint<AvailabilityType> dataPoint = new DataPoint<>((long) i, availabilityType);
                    MetricId<AvailabilityType> metricId = new MetricId<>(tenantId, AVAILABILITY, "avail");
                    Metric<AvailabilityType> metric = new Metric<>(metricId, ImmutableList.of(dataPoint));

                    metricsService.addDataPoints(AVAILABILITY, Observable.just(metric)).subscribe();

                    expected.add(metric);

                } else {
                    fail("Unexpected metric type: " + metricType);
                }
            }
        }

        assertTrue(latch.await(1, MINUTES), "Did not receive all notifications");

        assertEquals(actual.size(), expected.size());
        assertTrue(actual.containsAll(expected));
    }

    private String createRandomTenantId() {
        return UUID.randomUUID().toString();
    }
}
