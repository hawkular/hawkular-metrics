/*
 * Copyright 2014-2018 Red Hat, Inc. and/or its affiliates
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

import static org.hawkular.metrics.core.service.Functions.makeSafe;
import static org.hawkular.metrics.model.MetricType.AVAILABILITY;
import static org.hawkular.metrics.model.MetricType.GAUGE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.hawkular.metrics.core.service.Order;
import org.hawkular.metrics.datetime.DateTimeService;
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import rx.Observable;
import rx.observers.TestSubscriber;

public class MixedMetricsITest extends BaseMetricsITest {

    @Test
    public void createTenants() throws Exception {
        String t1Id = createRandomId();
        Tenant t1 = new Tenant(t1Id, ImmutableMap.of(GAUGE, 1, AVAILABILITY, 1));
        Tenant t2 = new Tenant(createRandomId(), ImmutableMap.of(GAUGE, 7));
        Tenant t3 = new Tenant(createRandomId(), ImmutableMap.of(AVAILABILITY, 2));
        Tenant t4 = new Tenant(createRandomId());

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
        String tenantId = createRandomId();
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
        String tenantId = createRandomId();
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

    // This is disabled since it fails inconsistently
    @Test(enabled = false)
    public void createAndDeleteMetrics() {
        createAndDeleteMetrics(MetricType.GAUGE, new Double[] { 1.2D, 2.3D, 3.4D, 4.5D });
        createAndDeleteMetrics(MetricType.COUNTER, new Long[] { 12L, 23L, 34L, 45L });
        createAndDeleteMetrics(MetricType.AVAILABILITY, new AvailabilityType[] { AvailabilityType.DOWN,
                AvailabilityType.UP, AvailabilityType.UP, AvailabilityType.ADMIN });
        createAndDeleteMetrics(MetricType.STRING, new String[] { "1.2d", "2.3d", "3.4d", "4.5d" });
    }

    private <T, V> void createAndDeleteMetrics(MetricType<T> mType, T[] dataPointValues) {
        String tenantId = createRandomId();
        final int iterations = 4;
        long now = DateTimeService.now.get().getMillis();

        List<Metric<T>> mList = new ArrayList<>();
        for (int i = 0; i < iterations; i++) {
            MetricId<T> mId = new MetricId<>(tenantId, mType, createRandomId());

            Map<String, String> tags = new HashMap<>();
            for (int j = 0; j < iterations; j++) {
                tags.put("test" + j, createRandomId());
            }

            List<DataPoint<T>> dataPoints = new ArrayList<>();
            for (int j = 0; j < dataPointValues.length; j++) {
                dataPoints.add(new DataPoint<T>(now + j, dataPointValues[j]));
            }

            Metric<T> m = new Metric<>(mId, tags, 21, dataPoints);
            mList.add(m);
            doAction(() -> metricsService.createMetric(m, true));
            doAction(() -> metricsService.addDataPoints(mType, Observable.just(m)));
        }

        List<Metric<T>> deletedMetrics = new ArrayList<>();
        for (Metric<T> m : mList) {
            MetricId<T> mId = m.getMetricId();

            Metric<T> actualMetric = metricsService.findMetric(mId).toBlocking().firstOrDefault(null);
            assertEquals(actualMetric.getMetricId(), mId);

            List<DataPoint<T>> actualDataPoints = metricsService.findDataPoints(mId, now, now+100, 100, Order.ASC)
                    .toList()
                    .toBlocking().firstOrDefault(null);
            assertEquals(actualDataPoints, m.getDataPoints());

            Map<String, String> actualTags = metricsService.getMetricTags(mId).toBlocking().lastOrDefault(null);
            assertEquals(actualTags, m.getTags());

            TestSubscriber<Void> deleteSubscriber = new TestSubscriber<>();
            metricsService.deleteMetric(mId).subscribe(deleteSubscriber);
            deleteSubscriber.awaitTerminalEvent(10, TimeUnit.SECONDS);
            deleteSubscriber.assertNoErrors();
            deleteSubscriber.assertCompleted();
            deletedMetrics.add(m);

            for (Metric<T> checkMetric : mList) {
                MetricId<T> checkId = checkMetric.getMetricId();

                actualMetric = metricsService.findMetric(checkId).toBlocking().firstOrDefault(null);
                if (deletedMetrics.contains(checkMetric)) {
                    assertEquals(actualMetric, null);
                } else {
                    assertEquals(actualMetric.getMetricId(), checkId);
                }

                actualDataPoints = metricsService.findDataPoints(checkId, now, now+100, 100, Order.ASC).toList()
                        .toBlocking().firstOrDefault(null);
                if (deletedMetrics.contains(checkMetric)) {
                    assertEquals(actualDataPoints.isEmpty(), true);
                } else {
                    assertEquals(actualDataPoints, checkMetric.getDataPoints());
                }

                actualTags = metricsService.getMetricTags(checkId).toBlocking().lastOrDefault(null);
                if (deletedMetrics.contains(checkMetric)) {
                    assertEquals(actualTags.isEmpty(), true);
                } else {
                    assertEquals(actualTags, checkMetric.getTags());
                }

                String testTagName = "test0";
                MetricId<T> taggedMetric = metricsService.findMetricIdentifiersWithFilters(checkMetric.getTenantId(),
                        checkMetric.getType(), testTagName + "= '" + checkMetric.getTags().get(testTagName) + "'")
                        .toBlocking()
                        .firstOrDefault(null);
                if (deletedMetrics.contains(checkMetric)) {
                    assertEquals(taggedMetric, null);
                } else {
                    assertEquals(taggedMetric, checkMetric.getMetricId());
                }

                int countFromTagIndex = dataAccess.findMetricsByTagNameValue(checkMetric.getTenantId(), testTagName,
                        checkMetric.getTags().get(testTagName))
                        .count().toBlocking().firstOrDefault(null);
                if (deletedMetrics.contains(checkMetric)) {
                    assertEquals(countFromTagIndex, 0);
                } else {
                    assertEquals(countFromTagIndex, 1);
                }
            }
        }
    }
}
