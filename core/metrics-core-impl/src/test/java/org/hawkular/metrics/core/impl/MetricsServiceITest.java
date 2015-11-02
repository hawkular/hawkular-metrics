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

package org.hawkular.metrics.core.impl;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MINUTES;

import static org.hawkular.metrics.core.api.AvailabilityType.DOWN;
import static org.hawkular.metrics.core.api.AvailabilityType.UNKNOWN;
import static org.hawkular.metrics.core.api.AvailabilityType.UP;
import static org.hawkular.metrics.core.api.MetricType.AVAILABILITY;
import static org.hawkular.metrics.core.api.MetricType.COUNTER;
import static org.hawkular.metrics.core.api.MetricType.COUNTER_RATE;
import static org.hawkular.metrics.core.api.MetricType.GAUGE;
import static org.hawkular.metrics.core.impl.Functions.makeSafe;
import static org.joda.time.DateTime.now;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.rank.PSquarePercentile;
import org.apache.commons.math3.stat.descriptive.summary.Sum;
import org.hawkular.metrics.core.api.Aggregate;
import org.hawkular.metrics.core.api.AvailabilityType;
import org.hawkular.metrics.core.api.Buckets;
import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricAlreadyExistsException;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.NumericBucketPoint;
import org.hawkular.metrics.core.api.Percentile;
import org.hawkular.metrics.core.api.Retention;
import org.hawkular.metrics.core.api.Tenant;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import rx.Observable;

/**
 * @author John Sanda
 */
public class MetricsServiceITest extends MetricsITest {

    private static final int DEFAULT_TTL = 7;    // 7 days

    private MetricsServiceImpl metricsService;

    private DataAccess dataAccess;

    private DateTimeService dateTimeService;

    private Function<Double, PercentileWrapper> defaultCreatePercentile;

    @BeforeClass
    public void initClass() {
        initSession();

        dataAccess = new DataAccessImpl(session, TestCacheManager.getCacheManager());

        dateTimeService = new DateTimeService();

        defaultCreatePercentile = NumericDataPointCollector.createPercentile;

        metricsService = new MetricsServiceImpl();
        metricsService.setDataAccess(dataAccess);
        metricsService.setTaskScheduler(new FakeTaskScheduler());
        metricsService.setDateTimeService(dateTimeService);
        metricsService.setDefaultTTL(DEFAULT_TTL);
        metricsService.startUp(session, getKeyspace(), false, new MetricRegistry());
    }

    @BeforeMethod
    public void initMethod() {
        session.execute("TRUNCATE tenants");
        session.execute("TRUNCATE data");
        session.execute("TRUNCATE metrics_idx");
        session.execute("TRUNCATE retentions_idx");
        session.execute("TRUNCATE metrics_tags_idx");
        session.execute("TRUNCATE tenants_by_time");
        metricsService.setDataAccess(dataAccess);
        NumericDataPointCollector.createPercentile = defaultCreatePercentile;
    }

    @Test
    public void createTenants() throws Exception {
        Tenant t1 = new Tenant("t1", ImmutableMap.of(GAUGE, 1, AVAILABILITY, 1));
        Tenant t2 = new Tenant("t2", ImmutableMap.of(GAUGE, 7));
        Tenant t3 = new Tenant("t3", ImmutableMap.of(AVAILABILITY, 2));
        Tenant t4 = new Tenant("t4");

        Observable.concat(
                metricsService.createTenant(t1),
                metricsService.createTenant(t2),
                metricsService.createTenant(t3),
                metricsService.createTenant(t4)
        ).toBlocking().lastOrDefault(null);

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

        assertDataRetentionsIndexMatches(t1.getId(), GAUGE, ImmutableSet.of(new Retention(
                new MetricId<>(t1.getId(), GAUGE, makeSafe(GAUGE.getText())), 1)));
        assertDataRetentionsIndexMatches(t1.getId(), AVAILABILITY, ImmutableSet.of(new Retention(
                new MetricId<>(t1.getId(), AVAILABILITY, makeSafe(AVAILABILITY.getText())), 1)));
    }

    @Test
    public void createMetricsIdxTenants() throws Exception {
        Metric<Double> em1 = new Metric<>(new MetricId<Double>("t123", GAUGE, "em1"));
        metricsService.createMetric(em1).toBlocking().lastOrDefault(null);

        Metric<Double> actual = metricsService.findMetric(em1.getId())
                .toBlocking()
                .lastOrDefault(null);
        assertNotNull(actual);
        assertEquals(actual, em1, "The metric does not match the expected value");

        Set<Tenant> actualTenants = ImmutableSet.copyOf(metricsService.getTenants().toBlocking().toIterable());
        assertEquals(actualTenants.size(), 1);

        Tenant t1 = new Tenant("t123", ImmutableMap.of(GAUGE, 1, AVAILABILITY, 1));
        metricsService.createTenant(t1).toBlocking().lastOrDefault(null);

        actualTenants = ImmutableSet.copyOf(metricsService.getTenants().toBlocking().toIterable());
        assertEquals(actualTenants.size(), 1);
    }

    @Test
    public void createAndFindMetrics() throws Exception {
        Metric<Double> em1 = new Metric<>(new MetricId<>("t1", GAUGE, "em1"));
        metricsService.createMetric(em1).toBlocking().lastOrDefault(null);
        Metric<Double> actual = metricsService.findMetric(em1.getId())
                .toBlocking()
                .lastOrDefault(null);
        assertNotNull(actual);
        assertEquals(actual, em1, "The metric does not match the expected value");

        Metric<Double> m1 = new Metric<>(new MetricId<>("t1", GAUGE, "m1"), ImmutableMap.of("a1", "1", "a2", "2"),
                24, 1);
        metricsService.createMetric(m1).toBlocking().lastOrDefault(null);

        actual = metricsService.findMetric(m1.getId()).toBlocking().last();
        assertEquals(actual, m1, "The metric does not match the expected value");

        Metric<AvailabilityType> m2 = new Metric<>(new MetricId<>("t1", AVAILABILITY, "m2"),
                                                              ImmutableMap.of("a3", "3", "a4", "3"), DEFAULT_TTL, 1);
        metricsService.createMetric(m2).toBlocking().lastOrDefault(null);

        // Find definitions with given tags
        Map<String, String> tagMap = new HashMap<>();
        tagMap.putAll(ImmutableMap.of("a1", "1", "a2", "2"));
        tagMap.putAll(ImmutableMap.of("a3", "3", "a4", "3"));

        // Test that distinct filtering does not remove same name from different types
        Metric<Double> gm2 = new Metric<>(new MetricId<>("t1", GAUGE, "m2"),
                ImmutableMap.of("a3", "3", "a4", "3"), null, null);
        metricsService.createMetric(gm2).toBlocking().lastOrDefault(null);

        Metric<AvailabilityType> actualAvail = metricsService.findMetric(m2.getId()).toBlocking()
                .last();
        assertEquals(actualAvail, m2, "The metric does not match the expected value");

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
        metricsService.createMetric(m1).subscribe(
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

        Metric<Double> m3 = new Metric<>(new MetricId<>("t1", GAUGE, "m3"), emptyMap(), 24, 2);
        metricsService.createMetric(m3).toBlocking().lastOrDefault(null);

        Metric<Double> m4 = new Metric<>(new MetricId<>("t1", GAUGE, "m4"),
                ImmutableMap.of("a1", "A", "a2", ""), null, null);
        metricsService.createMetric(m4).toBlocking().lastOrDefault(null);

        assertMetricIndexMatches("t1", GAUGE, asList(em1, m1, gm2, m3, m4));
        assertMetricIndexMatches("t1", AVAILABILITY, singletonList(m2));

        assertDataRetentionsIndexMatches("t1", GAUGE, ImmutableSet.of(new Retention(m3.getId(), 24),
                                                                      new Retention(m1.getId(), 24)));

        assertMetricsTagsIndexMatches("t1", "a1", asList(
                new MetricsTagsIndexEntry("1", m1.getId()),
                new MetricsTagsIndexEntry("A", m4.getId())
        ));
    }

    @Test
    public void createAndFindMetricsWithTags() throws Exception {

        String tenantId = "t1";

        // Create the gauges
        Metric<Double> m1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m1"), ImmutableMap.of("a1", "1"), 24, 2);
        Metric<Double> m2 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m2"), ImmutableMap.of("a1", "2", "a3", "3")
                , 24, 2);
        Metric<Double> m3 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m3"), ImmutableMap.of("a1", "2", "a2", "2")
                , 24, 2);
        Metric<Double> m4 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m4"), ImmutableMap.of("a1", "2", "a2", "3")
                , 24, 2);
        Metric<Double> m5 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m5"), ImmutableMap.of("a1", "2", "a2", "4")
                , 24, 2);
        Metric<Double> m6 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m6"), ImmutableMap.of("a2", "4"), 24, 2);

        Metric<Double> mA = new Metric<>(new MetricId<>(tenantId, GAUGE, "mA"), ImmutableMap.of("hostname", "webfin01"),
                                         24, 2);
        Metric<Double> mB = new Metric<>(new MetricId<>(tenantId, GAUGE, "mB"), ImmutableMap.of("hostname", "webswe02"),
                                         24, 2);
        Metric<Double> mC = new Metric<>(new MetricId<>(tenantId, GAUGE, "mC"), ImmutableMap.of("hostname",
                                                                                              "backendfin01"),
                                         24, 2);
        Metric<Double> mD = new Metric<>(new MetricId<>(tenantId, GAUGE, "mD"), ImmutableMap.of("hostname",
                                                                                              "backendswe02"),
                                         24, 7);
        Metric<Double> mE = new Metric<>(new MetricId<>(tenantId, GAUGE, "mE"), ImmutableMap.of("owner", "hede"),
                                         24, 7);
        Metric<Double> mF = new Metric<>(new MetricId<>(tenantId, GAUGE, "mF"), ImmutableMap.of("owner", "hades"),
                                         24, 7);
        Metric<Double> mG = new Metric<>(new MetricId<>(tenantId, GAUGE, "mG"), ImmutableMap.of("owner", "had"),
                                         24, 7);


        // Create the availabilities
        Metric<AvailabilityType> a1 = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, "a1"),
                                                   ImmutableMap.of("a1","4"), 24, 7);

        // Insert gauges
        metricsService.createMetric(m1).toBlocking().lastOrDefault(null);
        metricsService.createMetric(m2).toBlocking().lastOrDefault(null);
        metricsService.createMetric(m3).toBlocking().lastOrDefault(null);
        metricsService.createMetric(m4).toBlocking().lastOrDefault(null);
        metricsService.createMetric(m5).toBlocking().lastOrDefault(null);
        metricsService.createMetric(m6).toBlocking().lastOrDefault(null);
        metricsService.createMetric(a1).toBlocking().lastOrDefault(null);

        metricsService.createMetric(mA).toBlocking().lastOrDefault(null);
        metricsService.createMetric(mB).toBlocking().lastOrDefault(null);
        metricsService.createMetric(mC).toBlocking().lastOrDefault(null);
        metricsService.createMetric(mD).toBlocking().lastOrDefault(null);
        metricsService.createMetric(mE).toBlocking().lastOrDefault(null);
        metricsService.createMetric(mF).toBlocking().lastOrDefault(null);
        metricsService.createMetric(mG).toBlocking().lastOrDefault(null);

        // Check different scenarios..
        List<Metric<Double>> gauges = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a1", "*"), GAUGE)
                .toList()
                .toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 5, "Metrics m1-m5 should have been returned");

        gauges = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a1", "*", "a2", "2"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 1, "Only metric m3 should have been returned");

        gauges = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a1", "*", "a2", "2|3"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 2, "Metrics m3-m4 should have been returned");

        gauges = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a2", "2|3"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 2, "Metrics m3-m4 should have been returned");

        gauges = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a1", "*", "a2", "*"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 3, "Metrics m3-m5 should have been returned");

        gauges = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a1", "*", "a5", "*"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 0, "No gauges should have been returned");

        gauges = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a4", "*", "a5", "none"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 0, "No gauges should have been returned");

        List<Metric<Object>> metrics = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a1", "*"), null)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(metrics.size(), 6, "Metrics m1-m5 and a1 should have been returned");

        // Test that we actually get correct gauges also, not just correct size
        gauges = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a1", "2", "a2", "2"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 1, "Only metric m3 should have been returned");
        assertEquals(gauges.get(0), m3, "m3 did not match the original inserted metric");

        // Test for NOT operator
        gauges = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a2", "!4"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 2, "Only gauges m3-m4 should have been returned");

        gauges = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a1", "2", "a2", "!4"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 2, "Only gauges m3-m4 should have been returned");

        gauges = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a2", "!4|3"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 1, "Only gauges m3 should have been returned");
        assertEquals(gauges.get(0), m3, "m3 did not match the original inserted metric");

        // What about incorrect query?
        try {
            metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a2","**"), GAUGE)
                    .toList().toBlocking().lastOrDefault(null);
            fail("Should have thrown an PatternSyntaxException");
        } catch (PatternSyntaxException ignored) {
        }

        // More regexp tests
        gauges = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("hostname", "web.*"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 2, "Only websrv01 and websrv02 should have been returned");

        gauges = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("hostname", ".*01"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 2, "Only websrv01 and backend01 should have been returned");

        gauges = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("owner", "h[e|a]de(s?)"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(gauges.size(), 2, "Both hede and hades should have been returned, but not 'had'");
    }

    @Test
    public void createBasicCounterMetric() throws Exception {
        String tenantId = "counter-tenant";
        String name = "basic-counter";
        MetricId<Long> id = new MetricId<>(tenantId, COUNTER, name);

        Metric<Long> counter = new Metric<>(id);
        metricsService.createMetric(counter).toBlocking().lastOrDefault(null);

        Metric<Long> actual = metricsService.findMetric(id).toBlocking().lastOrDefault(null);

        assertEquals(actual, counter, "The counter metric does not match");
        assertMetricIndexMatches(tenantId, COUNTER, singletonList(counter));
    }

    @Test
    public void createCounterWithTags() throws Exception {
        String tenantId = "counter-tenant";
        String name = "tags-counter";
        MetricId<Long> id = new MetricId<>(tenantId, COUNTER, name);
        Map<String, String> tags = ImmutableMap.of("x", "1", "y", "2");

        Metric<Long> counter = new Metric<>(id, tags, null, null);
        metricsService.createMetric(counter).toBlocking().lastOrDefault(null);

        Metric<Long> actual = metricsService.findMetric(id).toBlocking().lastOrDefault(null);
        assertEquals(actual, counter, "The counter metric does not match");

        assertMetricIndexMatches(tenantId, COUNTER, singletonList(counter));
        assertMetricsTagsIndexMatches(tenantId, "x", singletonList(new MetricsTagsIndexEntry("1", id)));
    }

    @Test
    public void createCounterWithDataRetention() throws Exception {
        String tenantId = "counter-tenant";
        String name = "retention-counter";
        MetricId<Long> id = new MetricId<>(tenantId, COUNTER, name);
        Integer retention = 100;

        Metric<Long> counter = new Metric<>(id, emptyMap(), retention, null);
        metricsService.createMetric(counter).toBlocking().lastOrDefault(null);

        Metric<Long> actual = metricsService.findMetric(id).toBlocking().lastOrDefault(null);
        assertEquals(actual, counter, "The counter metric does not match");

        assertMetricIndexMatches(tenantId, COUNTER, singletonList(counter));
        assertDataRetentionsIndexMatches(tenantId, COUNTER, ImmutableSet.of(new Retention(id, retention)));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void doNotAllowCreationOfCounterRateMetric() {
        metricsService.createMetric(new Metric<>(new MetricId<>("test", COUNTER_RATE, "counter-rate"))).toBlocking()
                .lastOrDefault(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void doNotAllowInvalidMetricTags() {
        Map<String, String> invalidTagMap = ImmutableMap.of("key1", "value1", "", "value2");

        metricsService.addTags(new Metric<>(new MetricId<>("test", COUNTER_RATE, "counter-rate")), invalidTagMap)
                .toBlocking()
                .lastOrDefault(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void doNotAllowEmptyMetricTags() {
        metricsService.addTags(new Metric<>(new MetricId<>("test", COUNTER_RATE, "counter-rate")), null)
                .toBlocking()
                .lastOrDefault(null);
    }

    @Test
    public void updateMetricTags() throws Exception {
        Metric<Double> metric = new Metric<>(new MetricId<>("t1", GAUGE, "m1"),
                ImmutableMap.of("a1", "1", "a2", "2"), DEFAULT_TTL, 1);
        metricsService.createMetric(metric).toBlocking().lastOrDefault(null);

        Map<String, String> additions = ImmutableMap.of("a2", "two", "a3", "3");
        metricsService.addTags(metric, additions).toBlocking().lastOrDefault
                (null);

        Map<String, String> deletions = ImmutableMap.of("a1", "1");
        metricsService.deleteTags(metric, deletions).toBlocking()
                .lastOrDefault(null);

        Metric<?> updatedMetric = metricsService.findMetric(metric.getId()).toBlocking()
                .last();

        assertEquals(updatedMetric.getTags(), ImmutableMap.of("a2", "two", "a3", "3"),
                "The updated meta data does not match the expected values");

        assertMetricIndexMatches(metric.getId().getTenantId(), GAUGE, singletonList(updatedMetric));
    }

    @Test
    public void addAndFetchGaugeData() throws Exception {
        DateTime start = now().minusMinutes(30);
        DateTime end = start.plusMinutes(20);
        String tenantId = "t1";

        metricsService.createTenant(new Tenant(tenantId)).toBlocking().lastOrDefault(null);

        Metric<Double> m1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m1"), asList(
                new DataPoint<>(start.getMillis(), 1.1),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 3.3),
                new DataPoint<>(end.getMillis(), 4.4)
        ));

        Observable<Void> insertObservable = metricsService.addDataPoints(GAUGE, Observable.just(m1));
        insertObservable.toBlocking().lastOrDefault(null);

        Observable<DataPoint<Double>> observable = metricsService.findDataPoints(new MetricId<>(tenantId, GAUGE, "m1"),
                start.getMillis(), end.getMillis());
        List<DataPoint<Double>> actual = toList(observable);
        List<DataPoint<Double>> expected = asList(
                new DataPoint<>(start.plusMinutes(4).getMillis(), 3.3),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2),
                new DataPoint<>(start.getMillis(), 1.1)
        );

        assertEquals(actual, expected, "The data does not match the expected values");
        assertMetricIndexMatches("t1", GAUGE, singletonList(m1));
    }

    @Test
    public void addAndFetchCounterData() throws Exception {
        DateTime start = now().minusMinutes(30);
        DateTime end = start.plusMinutes(20);
        String tenantId = "counters-tenant";

        doAction(() -> metricsService.createTenant(new Tenant(tenantId)));

        Metric<Long> counter = new Metric<>(new MetricId<>(tenantId, COUNTER, "c1"), asList(
                new DataPoint<>(start.getMillis(), 10L),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 15L),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 25L),
                new DataPoint<>(end.getMillis(), 45L)
        ));

        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.just(counter)));

        Observable<DataPoint<Long>> data = metricsService.findDataPoints(new MetricId<>(tenantId, COUNTER, "c1"),
                start.getMillis(), end.getMillis());
        List<DataPoint<Long>> actual = toList(data);
        List<DataPoint<Long>> expected = asList(
                new DataPoint<>(start.getMillis(), 10L),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 15L),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 25L)
        );

        assertEquals(actual, expected, "The counter data does not match the expected values");
        assertMetricIndexMatches(tenantId, COUNTER, singletonList(counter));
    }

    @Test
    public void addAndFetchGaugeDataAggregates() throws Exception {
        DateTime start = now().minusMinutes(30);
        DateTime end = start.plusMinutes(20);
        String tenantId = "t1";

        metricsService.createTenant(new Tenant(tenantId)).toBlocking().lastOrDefault(null);

        Metric<Double> m1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m1"), asList(
                new DataPoint<>(start.getMillis(), 10.0),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 20.0),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 30.0),
                new DataPoint<>(end.getMillis(), 40.0)
                ));

        Observable<Void> insertObservable = metricsService.addDataPoints(GAUGE, Observable.just(m1));
        insertObservable.toBlocking().lastOrDefault(null);

        @SuppressWarnings("unchecked")
        Observable<Double> observable = metricsService
                .findGaugeData(new MetricId<Double>(tenantId, GAUGE, "m1"), start.getMillis(), (end.getMillis() + 1000),
                        Aggregate.Min, Aggregate.Max, Aggregate.Average, Aggregate.Sum);
        List<Double> actual = toList(observable);
        List<Double> expected = asList(10.0, 40.0, 25.0, 100.0);

        assertEquals(actual, expected, "The data does not match the expected values");
        assertMetricIndexMatches("t1", GAUGE, singletonList(m1));
    }

    @Test
    public void findCounterStats() {
        String tenantId = "counter-stats-test";

        Metric<Long> counter = new Metric<>(new MetricId<>(tenantId, COUNTER, "C1"), asList(
                new DataPoint<>((long) (60_000 * 1.0), 0L),
                new DataPoint<>((long) (60_000 * 1.5), 200L),
                new DataPoint<>((long) (60_000 * 3.5), 400L),
                new DataPoint<>((long) (60_000 * 5.0), 550L),
                new DataPoint<>((long) (60_000 * 7.0), 950L),
                new DataPoint<>((long) (60_000 * 7.5), 1000L)
        ));

        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.just(counter)));

        List<NumericBucketPoint> actual = metricsService.findCounterStats(counter.getId(),
                0, now().getMillis(), Buckets.fromStep(60_000, 60_000 * 8, 60_000),
                asList(95.0)).toBlocking().single();
        List<NumericBucketPoint> expected = new ArrayList<>();
        for (int i = 1; i < 8; i++) {
            NumericBucketPoint.Builder builder = new NumericBucketPoint.Builder(60_000 * i, 60_000 * (i + 1));
            switch (i) {
                case 1:
                    builder.setAvg(100D).setMax(200D).setMedian(0D).setMin(0D).setPercentiles(asList(new
                            Percentile(0.95, 0D))).setSamples(2);
                    break;
                case 2:
                case 4:
                case 6:
                    break;
                case 3:
                    builder.setAvg(400D).setMax(400D).setMedian(400D).setMin(400D).setPercentiles(asList(new
                            Percentile(0.95, 400D))).setSamples(1);
                    break;
                case 5:
                    builder.setAvg(550D).setMax(550D).setMedian(550D).setMin(550D).setPercentiles(asList(new
                            Percentile(0.95, 550D)))
                            .setSamples(1);
                    break;
                case 7:
                    builder.setAvg(975D).setMax(1000D).setMedian(950D).setMin(950D).setPercentiles(asList(new
                            Percentile(0.95, 950D)))
                            .setSamples(2);
                    break;
            }
            expected.add(builder.build());
        }

        assertNumericBucketsEquals(actual, expected);
    }

    @Test
    public void testBucketPercentiles() {
        String tenantId = "counter-stats-test";

        int testSize = 100;
        List<DataPoint<Long>> counterList = new ArrayList<>(testSize);

        PSquarePercentile top = new PSquarePercentile(99.9);

        for(long i = 0; i < testSize; i++) {
            counterList.add(new DataPoint<Long>((long) 60000+i, i));
            top.increment(i);
        }

        List<Double> percentiles = asList(50.0, 90.0, 99.0, 99.9);

        Metric<Long> counter = new Metric<>(new MetricId<>(tenantId, COUNTER, "C1"), counterList);

        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.just(counter)));

        List<NumericBucketPoint> actual = metricsService.findCounterStats(counter.getId(),
                0, now().getMillis(), Buckets.fromStep(60_000, 60_100, 60_100), percentiles).toBlocking()
                .single();

        assertEquals(1, actual.size());

        NumericBucketPoint bucket = actual.get(0);

        assertEquals(testSize, bucket.getSamples());
        assertEquals(percentiles.size(), bucket.getPercentiles().size());
        assertEquals(top.getResult(), bucket.getPercentiles().get(3).getValue());
    }

    @Test
    public void findRates() {
        String tenantId = "counter-rate-test";

        Metric<Long> counter = new Metric<>(new MetricId<>(tenantId, COUNTER, "C1"), asList(
                new DataPoint<>((long) (60_000 * 1.0), 0L),
                new DataPoint<>((long) (60_000 * 1.5), 200L),
                new DataPoint<>((long) (60_000 * 3.5), 400L),
                new DataPoint<>((long) (60_000 * 5.0), 550L),
                new DataPoint<>((long) (60_000 * 7.0), 950L),
                new DataPoint<>((long) (60_000 * 7.5), 1000L)
        ));

        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.just(counter)));

        List<DataPoint<Double>> actual = getOnNextEvents(() -> metricsService.findRateData(counter.getId(),
                0, now().getMillis()));
        List<DataPoint<Double>> expected = asList(
                new DataPoint<>((long) (60_000 * 1.5), 400D),
                new DataPoint<>((long) (60_000 * 3.5), 100D),
                new DataPoint<>((long) (60_000 * 5.0), 100D),
                new DataPoint<>((long) (60_000 * 7.0), 200D),
                new DataPoint<>((long) (60_000 * 7.5), 100D)
        );

        assertEquals(actual, expected, "The rates do not match");
    }

    @Test
    public void findRatesWhenTherAreCounterResets() {
        String tenantId = "counter-reset-test";

        Metric<Long> counter = new Metric<>(new MetricId<>(tenantId, COUNTER, "C1"), asList(
                new DataPoint<>((long) (60_000 * 1.0), 1L),
                new DataPoint<>((long) (60_000 * 1.5), 2L),
                new DataPoint<>((long) (60_000 * 3.5), 3L),
                new DataPoint<>((long) (60_000 * 5.0), 1L),
                new DataPoint<>((long) (60_000 * 7.0), 2L),
                new DataPoint<>((long) (60_000 * 7.5), 3L),
                new DataPoint<>((long) (60_000 * 8.0), 1L),
                new DataPoint<>((long) (60_000 * 8.5), 2L),
                new DataPoint<>((long) (60_000 * 9.0), 3L)
        ));
        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.just(counter)));

        List<DataPoint<Double>> actual = getOnNextEvents(() -> metricsService.findRateData(counter.getId(),
                0, now().getMillis()));
        List<DataPoint<Double>> expected = asList(
                new DataPoint<>((long) (60_000 * 1.5), 2D),
                new DataPoint<>((long) (60_000 * 3.5), 0.5),
                new DataPoint<>((long) (60_000 * 7.0), 0.5),
                new DataPoint<>((long) (60_000 * 7.5), 2D),
                new DataPoint<>((long) (60_000 * 8.5), 2D),
                new DataPoint<>((long) (60_000 * 9.0), 2D)
        );

        assertEquals(actual, expected, "The rates do not match");
    }

    @Test
    public void findRateStats() {
        String tenantId = "counter-rate-stats-test";

        Metric<Long> counter = new Metric<>(new MetricId<>(tenantId, COUNTER, "C1"), asList(
                new DataPoint<>((long) (60_000 * 1.0), 0L),
                new DataPoint<>((long) (60_000 * 1.5), 200L),
                new DataPoint<>((long) (60_000 * 3.5), 400L),
                new DataPoint<>((long) (60_000 * 5.0), 550L),
                new DataPoint<>((long) (60_000 * 7.0), 950L),
                new DataPoint<>((long) (60_000 * 7.5), 1000L)
        ));

        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.just(counter)));

        List<NumericBucketPoint> actual = metricsService.findRateStats(counter.getId(),
                0, now().getMillis(), Buckets.fromStep(60_000, 60_000 * 8, 60_000),
                asList(99.0)).toBlocking().single();
        List<NumericBucketPoint> expected = new ArrayList<>();
        for (int i = 1; i < 8; i++) {
            NumericBucketPoint.Builder builder = new NumericBucketPoint.Builder(60_000 * i, 60_000 * (i + 1));
            double val;
            switch (i) {
                case 1:
                    val = 400D;
                    builder.setAvg(val).setMax(val).setMedian(val).setMin(val).setPercentiles(asList(new Percentile
                            (0.99, val))).setSamples(1);
                    break;
                case 3:
                    val = 100D;
                    builder.setAvg(val).setMax(val).setMedian(val).setMin(val).setPercentiles(asList(new Percentile
                            (0.99, val))).setSamples(1);
                    break;
                case 5:
                    val = 100;
                    builder.setAvg(val).setMax(val).setMedian(val).setMin(val).setPercentiles(asList(new Percentile
                            (0.99, val))).setSamples(1);
                case 6:
                    break;
                case 7:
                    builder.setAvg(150.0).setMax(200.0).setMin(100.0).setMedian(100.0).setPercentiles(
                            asList(new Percentile(0.99, 100.0))).setSamples(2);
                default:
                    break;
            }
            expected.add(builder.build());
        }

        assertNumericBucketsEquals(actual, expected);
    }

    @Test
    public void findSimpleGaugeStatsByTags() {
        NumericDataPointCollector.createPercentile = InMemoryPercentileWrapper::new;

        String tenantId = "findGaugeStatsByTags";
        DateTime start = now().minusMinutes(10);

        Metric<Double> m1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "M1"), asList(
                new DataPoint<>(start.getMillis(), 12.23),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 9.745),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 14.01),
                new DataPoint<>(start.plusMinutes(3).getMillis(), 16.18),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 18.94)
        ));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m1)));
        doAction(() -> metricsService.addTags(m1, ImmutableMap.of("type", "cpu_usage", "node", "server1")));

        Metric<Double> m2 = new Metric<>(new MetricId<>(tenantId, GAUGE, "M2"), asList(
                new DataPoint<>(start.getMillis(), 15.47),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 8.08),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 14.39),
                new DataPoint<>(start.plusMinutes(3).getMillis(), 17.76),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 17.502)
        ));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m2)));
        doAction(() -> metricsService.addTags(m2, ImmutableMap.of("type", "cpu_usage", "node", "server2")));

        Metric<Double> m3 = new Metric<>(new MetricId<>(tenantId, GAUGE, "M3"), asList(
                new DataPoint<>(start.getMillis(), 11.456),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 18.32)
        ));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m3)));
        doAction(() -> metricsService.addTags(m3, ImmutableMap.of("type", "cpu_usage", "node", "server3")));

        Buckets buckets = Buckets.fromCount(start.getMillis(), start.plusMinutes(5).getMillis(), 1);
        Map<String, String> tagFilters = ImmutableMap.of("type", "cpu_usage", "node", "server1|server2");

        List<List<NumericBucketPoint>> actual = getOnNextEvents(
                () -> metricsService.findNumericStats(tenantId, MetricType.GAUGE,
                        tagFilters, start.getMillis(), start.plusMinutes(5).getMillis(), buckets,
                        Collections.emptyList(), false));

        assertEquals(actual.size(), 1);

        List<NumericBucketPoint> expected = Arrays.asList(createSingleBucket(
                Stream.concat(m1.getDataPoints().stream(), m2.getDataPoints().stream()).collect(Collectors.toList()),
                start, start.plusMinutes(5)));

        assertNumericBucketsEquals(actual.get(0), expected);
    }

    @Test
    public void findSimpleCounterStats() {
        //Setup the counter data
        NumericDataPointCollector.createPercentile = InMemoryPercentileWrapper::new;

        Random r = new Random(123);
        List<Long> randomList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            randomList.add((long) r.nextInt(100));
        }
        Collections.sort(randomList);
        Iterator<Long> randoms = randomList.iterator();

        String tenantId = "findCounterStats";
        DateTime start = now().minusMinutes(10);

        Metric<Long> c1 = new Metric<>(new MetricId<>(tenantId, COUNTER, "C1"), asList(
                new DataPoint<>(start.getMillis(), 222L + randoms.next()),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 224L + randoms.next()),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 226L + randoms.next()),
                new DataPoint<>(start.plusMinutes(3).getMillis(), 228L + randoms.next()),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 229L + randoms.next())));
        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.just(c1)));
        doAction(() -> metricsService.addTags(c1, ImmutableMap.of("type", "counter_cpu_usage", "node", "server1")));

        Metric<Long> c2 = new Metric<>(new MetricId<>(tenantId, COUNTER, "C2"), asList(
                new DataPoint<>(start.getMillis(), 150L),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 153L + randoms.next()),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 156L + randoms.next()),
                new DataPoint<>(start.plusMinutes(3).getMillis(), 159L + randoms.next()),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 162L + randoms.next())));
        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.just(c2)));
        doAction(() -> metricsService.addTags(c2, ImmutableMap.of("type", "counter_cpu_usage", "node", "server2")));

        Metric<Long> c3 = new Metric<>(new MetricId<>(tenantId, COUNTER, "C3"), asList(
                new DataPoint<>(start.getMillis(), 11456L + randoms.next()),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 183332L + randoms.next())));
        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.just(c3)));
        doAction(() -> metricsService.addTags(c3, ImmutableMap.of("type", "counter_cpu_usage", "node", "server3")));

        Buckets buckets = Buckets.fromCount(start.getMillis(), start.plusMinutes(5).getMillis(), 1);
        Map<String, String> tagFilters = ImmutableMap.of("type", "counter_cpu_usage", "node", "server1|server2");

        List<DataPoint<Double>> c1Rate = getOnNextEvents(
                () -> metricsService.findRateData(c1.getId(), start.getMillis(), start.plusMinutes(5).getMillis()));

        List<DataPoint<Double>> c2Rate = getOnNextEvents(
                () -> metricsService.findRateData(c2.getId(), start.getMillis(), start.plusMinutes(5).getMillis()));

        //Test simple counter stats
        List<List<NumericBucketPoint>> actualCounterStatsByTag = getOnNextEvents(
                () -> metricsService.findNumericStats(tenantId, MetricType.COUNTER, tagFilters, start.getMillis(),
                        start.plusMinutes(5).getMillis(), buckets, Collections.emptyList(), false));
        assertEquals(actualCounterStatsByTag.size(), 1);

        List<List<NumericBucketPoint>> actualCounterStatsById = getOnNextEvents(
                () -> metricsService.findNumericStats(tenantId, MetricType.COUNTER, asList("C1", "C2"),
                        start.getMillis(), start.plusMinutes(5).getMillis(), buckets, Collections.emptyList(), false));
        assertEquals(actualCounterStatsById.size(), 1);

        List<NumericBucketPoint> expectedCounterStats = Arrays.asList(createSingleBucket(
                Stream.concat(c1.getDataPoints().stream(), c2.getDataPoints().stream()).collect(Collectors.toList()),
                start, start.plusMinutes(5)));

        assertNumericBucketsEquals(actualCounterStatsByTag.get(0), expectedCounterStats);
        assertNumericBucketsEquals(actualCounterStatsById.get(0), expectedCounterStats);

        //Test stacked counter stats
        List<List<NumericBucketPoint>> actualStackedCounterStatsByTag = getOnNextEvents(
                () -> metricsService.findNumericStats(tenantId, MetricType.COUNTER, tagFilters, start.getMillis(),
                        start.plusMinutes(5).getMillis(), buckets, Collections.emptyList(), true));
        assertEquals(actualStackedCounterStatsByTag.size(), 1);

        List<List<NumericBucketPoint>> actualStackedCounterStatsById = getOnNextEvents(
                () -> metricsService.findNumericStats(tenantId, MetricType.COUNTER, asList("C1", "C2"),
                        start.getMillis(), start.plusMinutes(5).getMillis(), buckets, Collections.emptyList(), true));
        assertEquals(actualStackedCounterStatsByTag.size(), 1);

        NumericBucketPoint collectorC1 = createSingleBucket(c1.getDataPoints(), start, start.plusMinutes(5));
        NumericBucketPoint collectorC2 = createSingleBucket(c2.getDataPoints(), start, start.plusMinutes(5));

        final Sum min = new Sum();
        final Sum average = new Sum();
        final Sum median = new Sum();
        final Sum max = new Sum();
        Observable.just(collectorC1, collectorC2).forEach(d -> {
            min.increment(d.getMin());
            max.increment(d.getMax());
            average.increment(d.getAvg());
            median.increment(d.getMedian());
        });
        NumericBucketPoint expectedStackedRateBucketPoint = new NumericBucketPoint.Builder(start.getMillis(),
                start.plusMinutes(5).getMillis())
                        .setMin(min.getResult())
                        .setMax(max.getResult())
                        .setAvg(average.getResult())
                        .setMedian(median.getResult())
                        .setSamples(2)
                        .build();
        List<NumericBucketPoint> expectedStackedCounterStatsList = new ArrayList<NumericBucketPoint>();
        expectedStackedCounterStatsList.add(expectedStackedRateBucketPoint);

        assertNumericBucketsEquals(actualStackedCounterStatsByTag.get(0), expectedStackedCounterStatsList);
        assertNumericBucketsEquals(actualStackedCounterStatsById.get(0), expectedStackedCounterStatsList);

        //Test simple counter rate stats
        List<List<NumericBucketPoint>> actualCounterRateStatsByTag = getOnNextEvents(
                () -> metricsService.findNumericStats(tenantId, MetricType.COUNTER_RATE, tagFilters, start.getMillis(),
                        start.plusMinutes(5).getMillis(), buckets, Collections.emptyList(), false));
        assertEquals(actualCounterRateStatsByTag.size(), 1);

        List<List<NumericBucketPoint>> actualCounterRateStatsById = getOnNextEvents(
                () -> metricsService.findNumericStats(tenantId, MetricType.COUNTER_RATE, asList("C1", "C2"),
                        start.getMillis(), start.plusMinutes(5).getMillis(), buckets, Collections.emptyList(), false));
        assertEquals(actualCounterRateStatsById.size(), 1);

        List<NumericBucketPoint> expectedCounterRateStats = Arrays.asList(createSingleBucket(
                Stream.concat(c1Rate.stream(), c2Rate.stream()).collect(Collectors.toList()),
                start, start.plusMinutes(5)));

        assertNumericBucketsEquals(actualCounterRateStatsByTag.get(0), expectedCounterRateStats);
        assertNumericBucketsEquals(actualCounterRateStatsById.get(0), expectedCounterRateStats);

        //Test stacked counter rate stats
        List<List<NumericBucketPoint>> actualStackedCounterRateStatsByTag = getOnNextEvents(
                () -> metricsService.findNumericStats(tenantId, MetricType.COUNTER_RATE, tagFilters, start.getMillis(),
                        start.plusMinutes(5).getMillis(), buckets, Collections.emptyList(), true));
        assertEquals(actualStackedCounterStatsByTag.size(), 1);

        List<List<NumericBucketPoint>> actualStackedCounterRateStatsById = getOnNextEvents(
                () -> metricsService.findNumericStats(tenantId, MetricType.COUNTER_RATE, asList("C1", "C2"),
                        start.getMillis(), start.plusMinutes(5).getMillis(), buckets, Collections.emptyList(), true));
        assertEquals(actualStackedCounterStatsByTag.size(), 1);

        NumericBucketPoint collectorC1Rate = createSingleBucket(c1Rate, start, start.plusMinutes(5));
        NumericBucketPoint collectorC2Rate = createSingleBucket(c2Rate, start, start.plusMinutes(5));

        final Sum counterRateMin = new Sum();
        final Sum counterRateMax = new Sum();
        final Sum counterRateAverage = new Sum();
        final Sum counterRateMedian = new Sum();
        Observable.just(collectorC1Rate, collectorC2Rate).forEach(d -> {
            counterRateMin.increment(d.getMin());
            counterRateMax.increment(d.getMax());
            counterRateAverage.increment(d.getAvg());
            counterRateMedian.increment(d.getMedian());
        });
        NumericBucketPoint expectedStackedCounterRateBucketPoint = new NumericBucketPoint.Builder(start.getMillis(),
                start.plusMinutes(5).getMillis())
                        .setMin(counterRateMin.getResult())
                        .setMax(counterRateMax.getResult())
                        .setAvg(counterRateAverage.getResult())
                        .setMedian(counterRateMedian.getResult())
                        .setSamples(2)
                        .build();
        List<NumericBucketPoint> expectedStackedCounterRateStatsList = new ArrayList<NumericBucketPoint>();
        expectedStackedCounterRateStatsList.add(expectedStackedCounterRateBucketPoint);

        assertNumericBucketsEquals(actualStackedCounterRateStatsByTag.get(0), expectedStackedCounterRateStatsList);
        assertNumericBucketsEquals(actualStackedCounterRateStatsById.get(0), expectedStackedCounterRateStatsList);
    }

    private <T extends Number> NumericBucketPoint createSingleBucket(List<DataPoint<T>> combinedData, DateTime start,
            DateTime end) {
        T expectedMin = combinedData.stream()
                .min((x, y) -> Double.compare(x.getValue().doubleValue(), y.getValue().doubleValue()))
                .get()
                .getValue();
        T expectedMax = combinedData.stream()
                .max((x, y) -> Double.compare(x.getValue().doubleValue(), y.getValue().doubleValue()))
                .get()
                .getValue();
        PercentileWrapper expectedMedian = NumericDataPointCollector.createPercentile.apply(50.0);
        Mean expectedAverage = new Mean();
        Sum expectedSamples = new Sum();
        combinedData.stream().forEach(arg -> {
            expectedMedian.addValue(arg.getValue().doubleValue());
            expectedAverage.increment(arg.getValue().doubleValue());
            expectedSamples.increment(1);
        });

        return new NumericBucketPoint.Builder(start.getMillis(), end.getMillis())
                .setMin(expectedMin.doubleValue())
                .setMax(expectedMax.doubleValue())
                .setAvg(expectedAverage.getResult())
                .setMedian(expectedMedian.getResult())
                .setSamples(new Double(expectedSamples.getResult()).intValue())
                .build();
    }

    @Test
    public void findStackedGaugeStatsByTags() {
        NumericDataPointCollector.createPercentile = InMemoryPercentileWrapper::new;

        String tenantId = "findGaugeStatsByTags";
        DateTime start = now().minusMinutes(10);

        Metric<Double> m1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "M1"), asList(
                new DataPoint<>(start.getMillis(), 12.23),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 9.745),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 14.01),
                new DataPoint<>(start.plusMinutes(3).getMillis(), 16.18),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 18.94)));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m1)));
        doAction(() -> metricsService.addTags(m1, ImmutableMap.of("type", "cpu_usage", "node", "server1")));

        Metric<Double> m2 = new Metric<>(new MetricId<>(tenantId, GAUGE, "M2"), asList(
                new DataPoint<>(start.getMillis(), 15.47),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 8.08),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 14.39),
                new DataPoint<>(start.plusMinutes(3).getMillis(), 17.76),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 17.502)));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m2)));
        doAction(() -> metricsService.addTags(m2, ImmutableMap.of("type", "cpu_usage", "node", "server2")));

        Metric<Double> m3 = new Metric<>(new MetricId<>(tenantId, GAUGE, "M3"), asList(
                new DataPoint<>(start.getMillis(), 11.456),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 18.32)));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m3)));
        doAction(() -> metricsService.addTags(m3, ImmutableMap.of("type", "cpu_usage", "node", "server3")));

        Buckets buckets = Buckets.fromCount(start.getMillis(), start.plusMinutes(5).getMillis(), 1);
        Map<String, String> tagFilters = ImmutableMap.of("type", "cpu_usage", "node", "server1|server2");

        List<List<NumericBucketPoint>> actual = getOnNextEvents(() -> metricsService.findNumericStats(tenantId,
                MetricType.GAUGE, tagFilters, start.getMillis(), start.plusMinutes(5).getMillis(), buckets,
                Collections.emptyList(), true));

        assertEquals(actual.size(), 1);

        NumericBucketPoint collectorM1 = createSingleBucket(m1.getDataPoints(), start, start.plusMinutes(5));
        NumericBucketPoint collectorM2 = createSingleBucket(m2.getDataPoints(), start, start.plusMinutes(5));

        final Sum min = new Sum();
        final Sum average = new Sum();
        final Sum median = new Sum();
        final Sum max = new Sum();
        Observable.just(collectorM1, collectorM2).forEach(d -> {
            min.increment(d.getMin());
            max.increment(d.getMax());
            average.increment(d.getAvg());
            median.increment(d.getMedian());
        });

        assertEquals(actual.get(0).get(0).getMin(), min.getResult());
        assertEquals(actual.get(0).get(0).getMax(), max.getResult());
        assertEquals(actual.get(0).get(0).getMedian(), median.getResult());
        assertEquals(actual.get(0).get(0).getAvg(), average.getResult());
    }

    @Test
    public void findSimpleGaugeStatsByMetricNames() {
        NumericDataPointCollector.createPercentile = InMemoryPercentileWrapper::new;

        String tenantId = "findGaugeStatsByMetricNames";
        DateTime start = now().minusMinutes(10);

        Metric<Double> m1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "M1"), asList(
                new DataPoint<>(start.getMillis(), 12.23),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 9.745),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 14.01),
                new DataPoint<>(start.plusMinutes(3).getMillis(), 16.18),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 18.94)
        ));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m1)));

        Metric<Double> m2 = new Metric<>(new MetricId<>(tenantId, GAUGE, "M2"), asList(
                new DataPoint<>(start.getMillis(), 15.47),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 8.08),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 14.39),
                new DataPoint<>(start.plusMinutes(3).getMillis(), 17.76),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 17.502)
        ));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m2)));

        Metric<Double> m3 = new Metric<>(new MetricId<>(tenantId, GAUGE, "M3"), asList(
                new DataPoint<>(start.getMillis(), 11.456),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 18.32)
        ));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m3)));

        Buckets buckets = Buckets.fromCount(start.getMillis(), start.plusMinutes(5).getMillis(), 1);

        List<List<NumericBucketPoint>> actual = getOnNextEvents(() -> metricsService.findNumericStats(tenantId,
                MetricType.GAUGE, asList("M1", "M2"), start.getMillis(), start.plusMinutes(5).getMillis(), buckets,
                Collections.emptyList(), false));

        assertEquals(actual.size(), 1);

        List<NumericBucketPoint> expected = Arrays.asList(createSingleBucket(
                Stream.concat(m1.getDataPoints().stream(), m2.getDataPoints().stream()).collect(Collectors.toList()),
                start, start.plusMinutes(5)));

        assertNumericBucketsEquals(actual.get(0), expected);
    }

    @Test
    public void findStackedGaugeStatsByMetricNames() {
        NumericDataPointCollector.createPercentile = InMemoryPercentileWrapper::new;

        String tenantId = "findGaugeStatsByMetricNames";
        DateTime start = now().minusMinutes(10);

        Metric<Double> m1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "M1"), asList(
                new DataPoint<>(start.getMillis(), 12.23),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 9.745),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 14.01),
                new DataPoint<>(start.plusMinutes(3).getMillis(), 16.18),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 18.94)));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m1)));

        Metric<Double> m2 = new Metric<>(new MetricId<>(tenantId, GAUGE, "M2"), asList(
                new DataPoint<>(start.getMillis(), 15.47),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 8.08),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 14.39),
                new DataPoint<>(start.plusMinutes(3).getMillis(), 17.76),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 17.502)));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m2)));

        Metric<Double> m3 = new Metric<>(new MetricId<>(tenantId, GAUGE, "M3"), asList(
                new DataPoint<>(start.getMillis(), 11.456),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 18.32)));
        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.just(m3)));

        Buckets buckets = Buckets.fromCount(start.getMillis(), start.plusMinutes(5).getMillis(), 1);

        List<List<NumericBucketPoint>> actual = getOnNextEvents(() -> metricsService.findNumericStats(tenantId,
                MetricType.GAUGE, asList("M1", "M2"), start.getMillis(), start.plusMinutes(5).getMillis(), buckets,
                Collections.emptyList(), true));

        assertEquals(actual.size(), 1);

        NumericBucketPoint collectorM1 = createSingleBucket(m1.getDataPoints(), start, start.plusMinutes(5));
        NumericBucketPoint collectorM2 = createSingleBucket(m2.getDataPoints(), start, start.plusMinutes(5));

        final Sum min = new Sum();
        final Sum average = new Sum();
        final Sum median = new Sum();
        final Sum max = new Sum();
        Observable.just(collectorM1, collectorM2).forEach(d -> {
            min.increment(d.getMin());
            max.increment(d.getMax());
            average.increment(d.getAvg());
            median.increment(d.getMedian());
        });

        assertEquals(actual.get(0).get(0).getMin(), min.getResult());
        assertEquals(actual.get(0).get(0).getMax(), max.getResult());
        assertEquals(actual.get(0).get(0).getMedian(), median.getResult());
        assertEquals(actual.get(0).get(0).getAvg(), average.getResult());
    }

    private static void assertNumericBucketsEquals(List<NumericBucketPoint> actual, List<NumericBucketPoint> expected) {
        String msg = String.format("%nExpected:%n%s%nActual:%n%s%n", expected, actual);
        assertEquals(actual.size(), expected.size(), msg);
        IntStream.range(0, actual.size()).forEach(i -> {
            NumericBucketPoint actualPoint = actual.get(i);
            NumericBucketPoint expectedPoint = expected.get(i);
            assertNumericBucketEquals(actualPoint, expectedPoint, msg);
        });
    }

    private static void assertNumericBucketEquals(NumericBucketPoint actual, NumericBucketPoint expected, String msg) {
        assertEquals(actual.getStart(), expected.getStart(), msg);
        assertEquals(actual.getEnd(), expected.getEnd(), msg);
        assertEquals(actual.isEmpty(), expected.isEmpty(), msg);
        if (!actual.isEmpty()) {
            assertEquals(actual.getAvg(), expected.getAvg(), 0.001, msg);
            assertEquals(actual.getMax(), expected.getMax(), 0.001, msg);
            assertEquals(actual.getMedian(), expected.getMedian(), 0.001, msg);
            assertEquals(actual.getMin(), expected.getMin(), 0.001, msg);
            assertEquals(actual.getSamples(), expected.getSamples(), 0, msg);
            for(int i = 0; i < expected.getPercentiles().size(); i++) {
                assertEquals(expected.getPercentiles().get(i).getQuantile(), actual.getPercentiles().get(i)
                        .getQuantile(), 0.001, msg);
                assertEquals(expected.getPercentiles().get(i).getValue(), actual.getPercentiles().get(i)
                        .getValue(), 0.001, msg);
            }
        }
    }

    @Test
    public void findRatesWhenNoDataIsFound() {
        DateTime start = dateTimeService.currentHour().minusHours(1);
        String tenantId = "counter-rate-test-no-data";

        Metric<Long> counter = new Metric<>(new MetricId<>(tenantId, COUNTER, "C1"), asList(
                new DataPoint<>(start.plusMinutes(1).getMillis(), 10L),
                new DataPoint<>(start.plusMinutes(1).plusSeconds(45).getMillis(), 17L),
                new DataPoint<>(start.plusMinutes(2).plusSeconds(10).getMillis(), 29L)
        ));

        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.just(counter)));

        List<DataPoint<Double>> actual = getOnNextEvents(() -> metricsService.findRateData(counter.getId(),
                start.plusMinutes(3).getMillis(), start.plusMinutes(5).getMillis()));

        assertEquals(actual.size(), 0, "The rates do not match");
    }

    @Test
    public void addGaugeDataForMultipleMetrics() throws Exception {
        DateTime start = now().minusMinutes(10);
        DateTime end = start.plusMinutes(8);
        String tenantId = "test-tenant";

        metricsService.createTenant(new Tenant(tenantId)).toBlocking().lastOrDefault(null);

        Metric<Double> m1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m1"), asList(
                new DataPoint<>(start.plusSeconds(30).getMillis(), 11.2),
                new DataPoint<>(start.getMillis(), 11.1)
        ));

        Metric<Double> m2 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m2"), asList(
                new DataPoint<>(start.plusSeconds(30).getMillis(), 12.2),
                new DataPoint<>(start.getMillis(), 12.1)
        ));

        Metric<Double> m3 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m3"));

        Metric<Double> m4 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m4"), emptyMap(), 24, 1, asList(
                new DataPoint<>(start.plusSeconds(30).getMillis(), 55.5),
                new DataPoint<>(end.getMillis(), 66.6)
        ));
        metricsService.createMetric(m4).toBlocking().lastOrDefault(null);

        metricsService.addDataPoints(GAUGE, Observable.just(m1, m2, m3, m4)).toBlocking().lastOrDefault(null);

        Observable<DataPoint<Double>> observable1 = metricsService.findDataPoints(m1.getId(),
                start.getMillis(), end.getMillis());
        assertEquals(toList(observable1), m1.getDataPoints(), "The gauge data for " + m1.getId() + " does not match");

        Observable<DataPoint<Double>> observable2 = metricsService.findDataPoints(m2.getId(),
                start.getMillis(), end.getMillis());
        assertEquals(toList(observable2), m2.getDataPoints(), "The gauge data for " + m2.getId() + " does not match");

        Observable<DataPoint<Double>> observable3 = metricsService.findDataPoints(m3.getId(),
                start.getMillis(), end.getMillis());
        assertTrue(toList(observable3).isEmpty(), "Did not expect to get back results for " + m3.getId());

        Observable<DataPoint<Double>> observable4 = metricsService.findDataPoints(m4.getId(),
                start.getMillis(), end.getMillis());
        Metric<Double> expected = new Metric<>(new MetricId<>(tenantId, GAUGE, "m4"), emptyMap(), 24, 1,
                singletonList(new DataPoint<>(start.plusSeconds(30).getMillis(), 55.5)));
        assertEquals(toList(observable4), expected.getDataPoints(),
                "The gauge data for " + m4.getId() + " does not match");

        assertMetricIndexMatches(tenantId, GAUGE, asList(m1, m2, m3, m4));
    }

    @Test
    public void addAvailabilityForMultipleMetrics() throws Exception {
        DateTime start = now().minusMinutes(10);
        DateTime end = start.plusMinutes(8);
        String tenantId = "test-tenant";

        metricsService.createTenant(new Tenant(tenantId)).toBlocking().lastOrDefault(null);

        Metric<AvailabilityType> m1 = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, "m1"), asList(
                new DataPoint<>(start.plusSeconds(10).getMillis(), UP),
                new DataPoint<>(start.plusSeconds(20).getMillis(), DOWN)
        ));
        Metric<AvailabilityType> m2 = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, "m2"), asList(
                new DataPoint<>(start.plusSeconds(15).getMillis(), DOWN),
                new DataPoint<>(start.plusSeconds(30).getMillis(), UP)
        ));
        Metric<AvailabilityType> m3 = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, "m3"));

        metricsService.addDataPoints(AVAILABILITY, Observable.just(m1, m2, m3)).toBlocking().lastOrDefault(null);

        List<DataPoint<AvailabilityType>> actual = metricsService.findDataPoints(m1.getId(),
                start.getMillis(), end.getMillis()).toList().toBlocking().last();
        assertEquals(actual, m1.getDataPoints(), "The availability data does not match expected values");

        actual = metricsService.findDataPoints(m2.getId(), start.getMillis(), end.getMillis())
                .toList().toBlocking().last();
        assertEquals(actual, m2.getDataPoints(), "The availability data does not match expected values");

        actual = metricsService.findDataPoints(m3.getId(), start.getMillis(), end.getMillis())
                .toList().toBlocking().last();
        assertEquals(actual.size(), 0, "Did not expect to get back results since there is no data for " + m3);

        Metric<AvailabilityType> m4 = new Metric<>(new MetricId<>(tenantId, AVAILABILITY, "m4"), emptyMap(), 24,
                1, asList(
                new DataPoint<>(start.plusMinutes(2).getMillis(), UP),
                new DataPoint<>(end.plusMinutes(2).getMillis(), UP)));
        metricsService.createMetric(m4).toBlocking().lastOrDefault(null);

        metricsService.addDataPoints(AVAILABILITY, Observable.just(m4)).toBlocking().lastOrDefault(null);

        actual = metricsService.findDataPoints(m4.getId(), start.getMillis(), end.getMillis())
                .toList().toBlocking().last();
        Metric<AvailabilityType> expected = new Metric<>(m4.getId(), emptyMap(), 24, 1,
                singletonList(new DataPoint<>(start.plusMinutes(2).getMillis(), UP)));
        assertEquals(actual, expected.getDataPoints(), "The availability data does not match expected values");

        assertMetricIndexMatches(tenantId, AVAILABILITY, asList(m1, m2, m3, m4));
    }

    @Test
    public void findDistinctAvailabilities() throws Exception {
        DateTime end = now();
        DateTime start = end.minusMinutes(20);
        String tenantId = "tenant1";
        MetricId<AvailabilityType> metricId = new MetricId<>("tenant1", AVAILABILITY, "A1");

        metricsService.createTenant(new Tenant(tenantId)).toBlocking().lastOrDefault(null);

        Metric<AvailabilityType> metric = new Metric<>(metricId, Collections.emptyMap(), null, 60, asList(
                new DataPoint<>(start.getMillis(), UP),
                new DataPoint<>(start.plusMinutes(1).getMillis(), DOWN),
                new DataPoint<>(start.plusMinutes(2).getMillis(), DOWN),
                new DataPoint<>(start.plusMinutes(3).getMillis(), UP),
                new DataPoint<>(start.plusMinutes(4).getMillis(), DOWN),
                new DataPoint<>(start.plusMinutes(5).getMillis(), UP),
                new DataPoint<>(start.plusMinutes(6).getMillis(), UP),
                new DataPoint<>(start.plusMinutes(7).getMillis(), UNKNOWN),
                new DataPoint<>(start.plusMinutes(8).getMillis(), UNKNOWN),
                new DataPoint<>(start.plusMinutes(9).getMillis(), DOWN),
                new DataPoint<>(start.plusMinutes(10).getMillis(), UP)
        ));

        metricsService.addDataPoints(AVAILABILITY, Observable.just(metric)).toBlocking().lastOrDefault(null);

        List<DataPoint<AvailabilityType>> actual = metricsService.findAvailabilityData(metricId,
                start.getMillis(), end.getMillis(), true).toList().toBlocking().lastOrDefault(null);

        List<DataPoint<AvailabilityType>> expected = asList(
            metric.getDataPoints().get(0),
            metric.getDataPoints().get(1),
            metric.getDataPoints().get(3),
            metric.getDataPoints().get(4),
            metric.getDataPoints().get(5),
            metric.getDataPoints().get(7),
            metric.getDataPoints().get(9),
            metric.getDataPoints().get(10)
        );

        assertEquals(actual, expected, "The availability data does not match the expected values");
    }

    @Test
    public void addGaugeDataToThePast() throws Exception {
        // Add metric data to the past enough so that it triggers different buckets with default buckets
        DateTime end = now();
        DateTime start = end.minusDays(7);
        String tenantId = "t1";
        MetricId<Double> hm11 = new MetricId<>(tenantId, GAUGE, "hm1");

        metricsService.createTenant(new Tenant(tenantId)).toBlocking().lastOrDefault(null);

        Metric<Double> hm1 = new Metric<>(hm11, asList(
                new DataPoint<>(start.getMillis(), 1.1),
                new DataPoint<>(start.plusDays(1).plusMinutes(2).getMillis(), 2.2),
                new DataPoint<>(start.plusDays(3).plusMinutes(4).getMillis(), 3.3),
                new DataPoint<>(end.getMillis(), 4.4)
        ));

        metricsService.addDataPoints(GAUGE, Observable.just(hm1)).toBlocking().lastOrDefault(null);

        List<DataPoint<Double>> actual =
                metricsService.findDataPoints(hm11, start.minusMinutes(1).getMillis(), end.plusMinutes(1).getMillis())
                        .toList().toBlocking().lastOrDefault(null);

        assertEquals(actual, hm1.getDataPoints(), "The data does not match the expected values");
        assertEquals(actual.size(), 4, "Amount of datapoints does not match");
    }

    @Test
    public void getPeriodsAboveThreshold() throws Exception {
        String tenantId = "test-tenant";
        DateTime start = now().minusMinutes(20);
        double threshold = 20.0;

        List<DataPoint<Double>> dataPoints = asList(
                new DataPoint<>(start.getMillis(), 14.0),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 18.0),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 21.0),
                new DataPoint<>(start.plusMinutes(3).getMillis(), 23.0),
                new DataPoint<>(start.plusMinutes(3).getMillis(), 23.0),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 20.0),
                new DataPoint<>(start.plusMinutes(5).getMillis(), 19.0),
                new DataPoint<>(start.plusMinutes(6).getMillis(), 22.0),
                new DataPoint<>(start.plusMinutes(6).getMillis(), 22.0),
                new DataPoint<>(start.plusMinutes(6).getMillis(), 22.0),
                new DataPoint<>(start.plusMinutes(7).getMillis(), 15.0),
                new DataPoint<>(start.plusMinutes(8).getMillis(), 31.0),
                new DataPoint<>(start.plusMinutes(9).getMillis(), 30.0),
                new DataPoint<>(start.plusMinutes(10).getMillis(), 31.0)
        );
        Metric<Double> m1 = new Metric<>(new MetricId<>(tenantId, GAUGE, "m1"), dataPoints);

        metricsService.addDataPoints(GAUGE, Observable.just(m1)).toBlocking().lastOrDefault(null);

        List<long[]> actual = metricsService.getPeriods(m1.getId(),
                value -> value > threshold, start.getMillis(), now().getMillis()).toBlocking().lastOrDefault(null);
        List<long[]> expected = asList(
            new long[] {start.plusMinutes(2).getMillis(), start.plusMinutes(3).getMillis()},
            new long[] {start.plusMinutes(6).getMillis(), start.plusMinutes(6).getMillis()},
            new long[] {start.plusMinutes(8).getMillis(), start.plusMinutes(10).getMillis()}
        );

        assertEquals(actual.size(), expected.size(), "The number of periods is wrong");
        for (int i = 0; i < expected.size(); ++i) {
            assertArrayEquals(actual.get(i), expected.get(i), "The period does not match the expected value");
        }
    }

    @Test
    public void shouldReceiveInsertedDataNotifications() throws Exception {
        String tenantId = "shouldReceiveInsertedDataNotifications";
        ImmutableList<MetricType<?>> metricTypes = ImmutableList.of(GAUGE, COUNTER, AVAILABILITY);
        AvailabilityType[] availabilityTypes = AvailabilityType.values();

        int numberOfPoints = 10;

        List<Metric<?>> actual = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(metricTypes.size() * numberOfPoints);
        metricsService.insertedDataEvents()
                .filter(metric -> metric.getId().getTenantId().equals(tenantId))
                .subscribe(metric -> {
                    actual.add(metric);
                    latch.countDown();
                });

        List<Metric<?>> expected = new ArrayList<>();
        for (MetricType<?> metricType : metricTypes) {
            for (int i = 0; i < numberOfPoints; i++) {
                if (metricType == GAUGE) {

                    DataPoint<Double> dataPoint = new DataPoint<>(i, (double) i);
                    MetricId<Double> metricId = new MetricId<>(tenantId, GAUGE, "gauge");
                    Metric<Double> metric = new Metric<>(metricId, ImmutableList.of(dataPoint));

                    metricsService.addDataPoints(GAUGE, Observable.just(metric)).subscribe();

                    expected.add(metric);

                } else if (metricType == COUNTER) {

                    DataPoint<Long> dataPoint = new DataPoint<>(i, (long) i);
                    MetricId<Long> metricId = new MetricId<>(tenantId, COUNTER, "counter");
                    Metric<Long> metric = new Metric<>(metricId, ImmutableList.of(dataPoint));

                    metricsService.addDataPoints(COUNTER, Observable.just(metric)).subscribe();

                    expected.add(metric);

                } else if (metricType == AVAILABILITY) {

                    AvailabilityType availabilityType = availabilityTypes[i % availabilityTypes.length];
                    DataPoint<AvailabilityType> dataPoint = new DataPoint<>(i, availabilityType);
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

    @Test
    public void updateTenantsBucketsWhenAddingGaugeData() {
        String tenant1 = "GaugesTenantBucketsTest-1";
        String tenant2 = "GaugesTenantBucketsTest-1";
        DateTime bucket1 = dateTimeService.getTimeSlice(now(), Duration.standardMinutes(30));
        DateTime bucket2 = bucket1.minusMinutes(30);

        Metric<Double> m1 = new Metric<>(new MetricId<>(tenant1, GAUGE, "M1"),
                singletonList(new DataPoint<>(bucket1.plusMinutes(1).getMillis(), 21.22)));
        Metric<Double> m2 = new Metric<>(new MetricId<>(tenant2, GAUGE, "M2"),
                singletonList(new DataPoint<>(bucket2.plusMinutes(4).getMillis(), 33.44)));

        doAction(() -> metricsService.addDataPoints(GAUGE, Observable.from(asList(m1, m2))));

        List<String> bucket1Tenants = getTenantsInBucket(bucket1.getMillis());
        List<String> bucket2Tenants = getTenantsInBucket(bucket2.getMillis());

        assertEquals(bucket1Tenants, singletonList(tenant1), "The tenants for bucket " + bucket1 + " do not match");
        assertEquals(bucket2Tenants, singletonList(tenant2), "The tenants for bucket " + bucket1 + " do not match");
    }

    @Test
    public void updateTenantsWhenAddingCounterData() {
        String tenant1 = "CounterTenantBucketTest-1";
        String tenant2 = "CounterTenantBucketTest-2";
        DateTime bucket1 = dateTimeService.getTimeSlice(now(), Duration.standardMinutes(30));
        DateTime bucket2 = bucket1.minusMinutes(30);

        Metric<Long> m1 = new Metric<>(new MetricId<>(tenant1, COUNTER, "C1"),
                singletonList(new DataPoint<>(bucket1.plusMinutes(5).getMillis(), 100L)));
        Metric<Long> m2 = new Metric<>(new MetricId<>(tenant2, COUNTER, "C2"),
                singletonList(new DataPoint<>(bucket2.plusMinutes(10).getMillis(), 250L)));

        doAction(() -> metricsService.addDataPoints(COUNTER, Observable.from(asList(m1, m2))));

        List<String> bucket1Tenants = getTenantsInBucket(bucket1.getMillis());
        List<String> bucket2Tenants = getTenantsInBucket(bucket2.getMillis());

        assertEquals(bucket1Tenants, singletonList(tenant1), "The tenants for bucket " + bucket1 + " do not match");
        assertEquals(bucket2Tenants, singletonList(tenant2), "The tenants for bucket " + bucket1 + " do not match");
    }

    @Test
    public void updateTenantsWhenAddingAvailabilityData() {
        String tenant1 = "AvailabilityTenantBucketTest-1";
        String tenant2 = "AvailabilityTenantBucketTest-2";
        DateTime bucket1 = dateTimeService.getTimeSlice(now(), Duration.standardMinutes(30));
        DateTime bucket2 = bucket1.minusMinutes(30);

        Metric<AvailabilityType> m1 = new Metric<>(new MetricId<>(tenant1, AVAILABILITY, "A1"),
                singletonList(new DataPoint<>(bucket1.plusMinutes(5).getMillis(), UP)));
        Metric<AvailabilityType> m2 = new Metric<>(new MetricId<>(tenant2, AVAILABILITY, "A2"),
                singletonList(new DataPoint<>(bucket2.plusMinutes(10).getMillis(), DOWN)));

        doAction(() -> metricsService.addDataPoints(AVAILABILITY, Observable.from(asList(m1, m2))));

        List<String> bucket1Tenants = getTenantsInBucket(bucket1.getMillis());
        List<String> bucket2Tenants = getTenantsInBucket(bucket2.getMillis());

        assertEquals(bucket1Tenants, singletonList(tenant1), "The tenants for bucket " + bucket1 + " do not match");
        assertEquals(bucket2Tenants, singletonList(tenant2), "The tenants for bucket " + bucket1 + " do not match");
    }

    private List<String> getTenantsInBucket(long bucket) {
        return getOnNextEvents(() -> dataAccess.findTenantIds(bucket)
                .flatMap(Observable::from)
                .map(row -> row.getString(0)));
    }

    private <T> List<T> toList(Observable<T> observable) {
        return ImmutableList.copyOf(observable.toBlocking().toIterable());
    }

    private void assertMetricIndexMatches(String tenantId, MetricType<?> type, List<Metric<?>> expected)
        throws Exception {
        List<Metric<?>> actualIndex = ImmutableList.copyOf(metricsService.findMetrics(tenantId, type).toBlocking()
                .toIterable());
        assertEquals(actualIndex, expected, "The metrics index results do not match");
    }

    private void assertArrayEquals(long[] actual, long[] expected, String msg) {
        assertEquals(actual.length, expected.length, msg + ": The array lengths are not the same.");
        for (int i = 0; i < expected.length; ++i) {
            assertEquals(actual[i], expected[i], msg + ": The elements at index " + i + " do not match.");
        }
    }

    private class MetricsTagsIndexEntry {
        String tagValue;
        MetricId<?> id;

        public MetricsTagsIndexEntry(String tagValue, MetricId<?> id) {
            this.tagValue = tagValue;
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MetricsTagsIndexEntry that = (MetricsTagsIndexEntry) o;

            if (!id.equals(that.id)) return false;
            if (!tagValue.equals(that.tagValue)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = tagValue.hashCode();
            result = 31 * result + id.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return com.google.common.base.Objects.toStringHelper(this)
                .add("tagValue", tagValue)
                .add("id", id)
                .toString();
        }
    }

    private void assertMetricsTagsIndexMatches(String tenantId, String tag, List<MetricsTagsIndexEntry> expected)
        throws Exception {
        ResultSet resultSet = dataAccess.findMetricsByTagName(tenantId, tag).toBlocking().first();
        List<MetricsTagsIndexEntry> actual = new ArrayList<>();

        for (Row row : resultSet) {
            MetricType<?> type = MetricType.fromCode(row.getByte(0));
            MetricId<?> id = new MetricId<>(tenantId, type, row.getString(1));
            actual.add(new MetricsTagsIndexEntry(row.getString(2), id)); // Need value here.. pff.
        }

        assertEquals(actual, expected, "The metrics tags index entries do not match");
    }

    private void assertDataRetentionsIndexMatches(String tenantId, MetricType<?> type, Set<Retention> expected)
        throws Exception {
        ResultSetFuture queryFuture = dataAccess.findDataRetentions(tenantId, type);
        ListenableFuture<Set<Retention>> retentionsFuture = Futures.transform(queryFuture,
                new DataRetentionsMapper(tenantId, type));
        Set<Retention> actual = getUninterruptibly(retentionsFuture);

        assertEquals(actual, expected, "The data retentions are wrong");
    }

    private static class InMemoryPercentileWrapper implements PercentileWrapper {
        List<Double> values = new ArrayList<>();
        double percentile;

        public InMemoryPercentileWrapper(double percentile) {
            this.percentile = percentile;
        }

        @Override public void addValue(double value) {
            values.add(value);
        }

        @Override public double getResult() {
            org.apache.commons.math3.stat.descriptive.rank.Percentile percentileCalculator =
                    new org.apache.commons.math3.stat.descriptive.rank.Percentile(percentile);
            double[] array = new double[values.size()];
            for (int i = 0; i < array.length; ++i) {
                array[i] = values.get(i++);
            }
            percentileCalculator.setData(array);

            return percentileCalculator.getQuantile();
        }
    }
}
