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

import static org.hawkular.metrics.core.api.AvailabilityType.DOWN;
import static org.hawkular.metrics.core.api.AvailabilityType.UNKNOWN;
import static org.hawkular.metrics.core.api.AvailabilityType.UP;
import static org.hawkular.metrics.core.api.MetricType.AVAILABILITY;
import static org.hawkular.metrics.core.api.MetricType.COUNTER;
import static org.hawkular.metrics.core.api.MetricType.COUNTER_RATE;
import static org.hawkular.metrics.core.api.MetricType.GAUGE;
import static org.hawkular.metrics.core.impl.DataAccessImpl.DPART;
import static org.hawkular.metrics.core.impl.Functions.makeSafe;
import static org.hawkular.metrics.core.impl.MetricsServiceImpl.DEFAULT_TTL;
import static org.hawkular.metrics.core.impl.TimeUUIDUtils.getTimeUUID;
import static org.joda.time.DateTime.now;
import static org.joda.time.Days.days;
import static org.joda.time.Hours.hours;
import static org.junit.Assert.fail;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.PatternSyntaxException;

import org.hawkular.metrics.core.api.Aggregate;
import org.hawkular.metrics.core.api.AvailabilityType;
import org.hawkular.metrics.core.api.DataPoint;
import org.hawkular.metrics.core.api.Interval;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricAlreadyExistsException;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.Retention;
import org.hawkular.metrics.core.api.Tenant;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
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

    private MetricsServiceImpl metricsService;

    private DataAccess dataAccess;

    private PreparedStatement insertGaugeDataWithTimestamp;

    private PreparedStatement insertAvailabilityDateWithTimestamp;

    @BeforeClass
    public void initClass() {
        initSession();

        metricsService = new MetricsServiceImpl();
        metricsService.setTaskScheduler(new FakeTaskScheduler());
        metricsService.startUp(session, getKeyspace(), false, new MetricRegistry());
        dataAccess = metricsService.getDataAccess();

        insertGaugeDataWithTimestamp = session
                .prepare(
            "INSERT INTO data (tenant_id, type, metric, interval, dpart, time, n_value) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?) " +
            "USING TTL ? AND TIMESTAMP ?");

        insertAvailabilityDateWithTimestamp = session.prepare(
            "INSERT INTO data (tenant_id, type, metric, interval, dpart, time, availability) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?) " +
            "USING TTL ? AND TIMESTAMP ?"
        );
    }

    @BeforeMethod
    public void initMethod() {
        session.execute("TRUNCATE tenants");
        session.execute("TRUNCATE data");
        session.execute("TRUNCATE tags");
        session.execute("TRUNCATE metrics_idx");
        session.execute("TRUNCATE retentions_idx");
        session.execute("TRUNCATE metrics_tags_idx");
        metricsService.setDataAccess(dataAccess);
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
                new MetricId(t1.getId(), GAUGE, makeSafe(GAUGE.getText())), 1)));
        assertDataRetentionsIndexMatches(t1.getId(), AVAILABILITY, ImmutableSet.of(new Retention(
                new MetricId(t1.getId(), AVAILABILITY, makeSafe(AVAILABILITY.getText())), 1)));
    }

    @Test
    public void createAndFindMetrics() throws Exception {
        Metric<Double> em1 = new Metric<>(new MetricId("t1", GAUGE, "em1"));
        metricsService.createMetric(em1).toBlocking().lastOrDefault(null);
        Metric actual = metricsService.findMetric(em1.getId())
                .toBlocking()
                .lastOrDefault(null);
        assertNotNull(actual);
        assertEquals(actual, em1, "The metric does not match the expected value");

        Metric<Double> m1 = new Metric<>(new MetricId("t1", GAUGE, "m1"), ImmutableMap.of("a1", "1", "a2", "2"), 24);
        metricsService.createMetric(m1).toBlocking().lastOrDefault(null);

        actual = metricsService.findMetric(m1.getId()).toBlocking().last();
        assertEquals(actual, m1, "The metric does not match the expected value");

        Metric<DataPoint<AvailabilityType>> m2 = new Metric<>(new MetricId("t1", AVAILABILITY, "m2"),
                                                              ImmutableMap.of("a3", "3", "a4", "3"), DEFAULT_TTL);
        metricsService.createMetric(m2).toBlocking().lastOrDefault(null);

        // Find definitions with given tags
        Map<String, String> tagMap = new HashMap<>();
        tagMap.putAll(ImmutableMap.of("a1", "1", "a2", "2"));
        tagMap.putAll(ImmutableMap.of("a3", "3", "a4", "3"));

        // Test that distinct filtering does not remove same name from different types
        Metric<DataPoint<AvailabilityType>> gm2 = new Metric<>(new MetricId("t1", GAUGE, "m2"),
                ImmutableMap.of("a3", "3", "a4", "3"), null);
        metricsService.createMetric(gm2).toBlocking().lastOrDefault(null);

        Metric actualAvail = metricsService.findMetric(m2.getId()).toBlocking()
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

        Metric<Double> m3 = new Metric<>(new MetricId("t1", GAUGE, "m3"), emptyMap(), 24);
        metricsService.createMetric(m3).toBlocking().lastOrDefault(null);

        Metric<Double> m4 = new Metric<>(new MetricId("t1", GAUGE, "m4"),
                ImmutableMap.of("a1", "A", "a2", ""), null);
        metricsService.createMetric(m4).toBlocking().lastOrDefault(null);

        assertMetricIndexMatches("t1", GAUGE, asList(em1, m1, gm2, m3, m4));
        assertMetricIndexMatches("t1", AVAILABILITY, singletonList(m2));

        assertDataRetentionsIndexMatches("t1", GAUGE, ImmutableSet.of(new Retention(m3.getId(), 24),
                                                                      new Retention(m1.getId(), 24)));

        assertMetricsTagsIndexMatches("t1", "a1", asList(
                new MetricsTagsIndexEntry("1", GAUGE, m1.getId()),
                new MetricsTagsIndexEntry("A", GAUGE, m4.getId())
        ));
    }

    @Test
    public void createAndFindMetricsWithTags() throws Exception {

        String tenantId = "t1";

        // Create the gauges
        Metric<Double> m1 = new Metric<>(new MetricId(tenantId, GAUGE, "m1"), ImmutableMap.of("a1","1"), 24);
        Metric<Double> m2 = new Metric<>(new MetricId(tenantId, GAUGE, "m2"), ImmutableMap.of("a1","2","a3","3"), 24);
        Metric<Double> m3 = new Metric<>(new MetricId(tenantId, GAUGE, "m3"), ImmutableMap.of("a1","2","a2","2"), 24);
        Metric<Double> m4 = new Metric<>(new MetricId(tenantId, GAUGE, "m4"), ImmutableMap.of("a1","2","a2","3"), 24);
        Metric<Double> m5 = new Metric<>(new MetricId(tenantId, GAUGE, "m5"), ImmutableMap.of("a1","2","a2","4"), 24);
        Metric<Double> m6 = new Metric<>(new MetricId(tenantId, GAUGE, "m6"), ImmutableMap.of("a2","4"), 24);

        Metric<Double> mA = new Metric<>(new MetricId(tenantId, GAUGE, "mA"), ImmutableMap.of("hostname","webfin01"),
                                         24);
        Metric<Double> mB = new Metric<>(new MetricId(tenantId, GAUGE, "mB"), ImmutableMap.of("hostname","webswe02"),
                                         24);
        Metric<Double> mC = new Metric<>(new MetricId(tenantId, GAUGE, "mC"), ImmutableMap.of("hostname",
                                                                                              "backendfin01"),
                                         24);
        Metric<Double> mD = new Metric<>(new MetricId(tenantId, GAUGE, "mD"), ImmutableMap.of("hostname",
                                                                                              "backendswe02"),
                                         24);
        Metric<Double> mE = new Metric<>(new MetricId(tenantId, GAUGE, "mE"), ImmutableMap.of("owner","hede"),
                                         24);
        Metric<Double> mF = new Metric<>(new MetricId(tenantId, GAUGE, "mF"), ImmutableMap.of("owner","hades"),
                                         24);
        Metric<Double> mG = new Metric<>(new MetricId(tenantId, GAUGE, "mG"), ImmutableMap.of("owner","had"),
                                         24);


        // Create the availabilities
        Metric<AvailabilityType> a1 = new Metric<>(new MetricId(tenantId, AVAILABILITY, "a1"),
                                                   ImmutableMap.of("a1","4"), 24);

        // Insert metrics
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
        List<Metric> metrics = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a1", "*"), GAUGE).toList()
                .toBlocking().lastOrDefault(null);
        assertEquals(metrics.size(), 5, "Metrics m1-m5 should have been returned");

        metrics = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a1","*","a2","2"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(metrics.size(), 1, "Only metric m3 should have been returned");

        metrics = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a1", "*", "a2", "2|3"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(metrics.size(), 2, "Metrics m3-m4 should have been returned");

        metrics = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a2", "2|3"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(metrics.size(), 2, "Metrics m3-m4 should have been returned");

        metrics = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a1", "*", "a2", "*"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(metrics.size(), 3, "Metrics m3-m5 should have been returned");

        metrics = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a1", "*", "a5", "*"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(metrics.size(), 0, "No metrics should have been returned");

        metrics = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a4", "*", "a5", "none"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(metrics.size(), 0, "No metrics should have been returned");

        metrics = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a1", "*"), null).toList()
                .toBlocking().lastOrDefault(null);
        assertEquals(metrics.size(), 6, "Metrics m1-m5 and a1 should have been returned");

        // Test that we actually get correct metrics also, not just correct size
        metrics = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a1","2","a2","2"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(metrics.size(), 1, "Only metric m3 should have been returned");
        assertEquals(metrics.get(0), m3, "m3 did not match the original inserted metric");

        // Test for NOT operator
        metrics = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a2","!4"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(metrics.size(), 2, "Only metrics m3-m4 should have been returned");

        metrics = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a1", "2", "a2","!4"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(metrics.size(), 2, "Only metrics m3-m4 should have been returned");

        metrics = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a2","!4|3"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(metrics.size(), 1, "Only metrics m3 should have been returned");
        assertEquals(metrics.get(0), m3, "m3 did not match the original inserted metric");

        // What about incorrect query?
        try {
            metricsService.findMetricsWithFilters("t1", ImmutableMap.of("a2","**"), GAUGE)
                    .toList().toBlocking().lastOrDefault(null);
            fail("Should have thrown an PatternSyntaxException");
        } catch(PatternSyntaxException e) { }

        // More regexp tests
        metrics = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("hostname","web.*"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(metrics.size(), 2, "Only websrv01 and websrv02 should have been returned");

        metrics = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("hostname",".*01"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(metrics.size(), 2, "Only websrv01 and backend01 should have been returned");

        metrics = metricsService.findMetricsWithFilters("t1", ImmutableMap.of("owner","h[e|a]de(s?)"), GAUGE)
                .toList().toBlocking().lastOrDefault(null);
        assertEquals(metrics.size(), 2, "Both hede and hades should have been returned, but not 'had'");
    }

    @Test
    public void createBasicCounterMetric() throws Exception {
        String tenantId = "counter-tenant";
        String name = "basic-counter";
        MetricId id = new MetricId(tenantId, COUNTER, name);

        Metric<?> counter = new Metric<>(id);
        metricsService.createMetric(counter).toBlocking().lastOrDefault(null);

        Metric<?> actual = metricsService.findMetric(id).toBlocking().lastOrDefault(null);

        assertEquals(actual, counter, "The counter metric does not match");
        assertMetricIndexMatches(tenantId, COUNTER, singletonList(counter));
    }

    @Test
    public void createCounterWithTags() throws Exception {
        String tenantId = "counter-tenant";
        String name = "tags-counter";
        MetricId id = new MetricId(tenantId, COUNTER, name);
        Map<String, String> tags = ImmutableMap.of("x", "1", "y", "2");

        Metric<?> counter = new Metric<>(id, tags, null);
        metricsService.createMetric(counter).toBlocking().lastOrDefault(null);

        Metric<?> actual = metricsService.findMetric(id).toBlocking().lastOrDefault(null);
        assertEquals(actual, counter, "The counter metric does not match");

        assertMetricIndexMatches(tenantId, COUNTER, singletonList(counter));
        assertMetricsTagsIndexMatches(tenantId, "x", singletonList(new MetricsTagsIndexEntry("1", COUNTER, id)));
    }

    @Test
    public void createCounterWithDataRetention() throws Exception {
        String tenantId = "counter-tenant";
        String name = "retention-counter";
        MetricId id = new MetricId(tenantId, COUNTER, name);
        Integer retention = 100;

        Metric<?> counter = new Metric<>(id, emptyMap(), retention);
        metricsService.createMetric(counter).toBlocking().lastOrDefault(null);

        Metric<?> actual = metricsService.findMetric(id).toBlocking().lastOrDefault(null);
        assertEquals(actual, counter, "The counter metric does not match");

        assertMetricIndexMatches(tenantId, COUNTER, singletonList(counter));
        assertDataRetentionsIndexMatches(tenantId, COUNTER, ImmutableSet.of(new Retention(id, retention)));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void doNotAllowCreationOfCounterRateMetric() {
        metricsService.createMetric(new Metric<>(new MetricId("test", COUNTER_RATE, "counter-rate"))).toBlocking()
                .lastOrDefault(null);
    }

    @Test
    public void updateMetricTags() throws Exception {
        Metric<Double> metric = new Metric<>(new MetricId("t1", GAUGE, "m1"),
                ImmutableMap.of("a1", "1", "a2", "2"), DEFAULT_TTL);
        metricsService.createMetric(metric).toBlocking().lastOrDefault(null);

        Map<String, String> additions = ImmutableMap.of("a2", "two", "a3", "3");
        metricsService.addTags(metric, additions).toBlocking().lastOrDefault
                (null);

        Map<String, String> deletions = ImmutableMap.of("a1", "1");
        metricsService.deleteTags(metric, deletions).toBlocking()
                .lastOrDefault(null);

        Metric updatedMetric = metricsService.findMetric(metric.getId()).toBlocking()
                .last();

        assertEquals(updatedMetric.getTags(), ImmutableMap.of("a2", "two", "a3", "3"),
                "The updated meta data does not match the expected values");

        assertMetricIndexMatches(metric.getTenantId(), GAUGE, singletonList(updatedMetric));
    }

    @Test
    public void addAndFetchGaugeData() throws Exception {
        DateTime start = now().minusMinutes(30);
        DateTime end = start.plusMinutes(20);
        String tenantId = "t1";

        metricsService.createTenant(new Tenant(tenantId)).toBlocking().lastOrDefault(null);

        Metric<Double> m1 = new Metric<>(new MetricId(tenantId, GAUGE, "m1"), asList(
                new DataPoint<>(start.getMillis(), 1.1),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 2.2),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 3.3),
                new DataPoint<>(end.getMillis(), 4.4)
        ));

        Observable<Void> insertObservable = metricsService.addGaugeData(Observable.just(m1));
        insertObservable.toBlocking().lastOrDefault(null);

        Observable<DataPoint<Double>> observable = metricsService.findGaugeData(new MetricId(tenantId, GAUGE, "m1"),
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

        metricsService.createTenant(new Tenant(tenantId)).toBlocking().lastOrDefault(null);

        Metric<Long> counter = new Metric<>(new MetricId(tenantId, COUNTER, "c1"), asList(
                new DataPoint<>(start.getMillis(), 10L),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 15L),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 25L),
                new DataPoint<>(end.getMillis(), 45L)
        ));

        metricsService.addCounterData(Observable.just(counter)).toBlocking().lastOrDefault(null);

        Observable<DataPoint<Long>> data = metricsService.findCounterData(new MetricId(tenantId, COUNTER, "c1"),
                start.getMillis(), end.getMillis());
        List<DataPoint<Long>> actual = toList(data);
        List<DataPoint<Long>> expected = asList(
                new DataPoint<>(start.plusMinutes(4).getMillis(), 25L),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 15L),
                new DataPoint<>(start.getMillis(), 10L)
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

        Metric<Double> m1 = new Metric<>(new MetricId(tenantId, GAUGE, "m1"), asList(
                new DataPoint<>(start.getMillis(), 10.0),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 20.0),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 30.0),
                new DataPoint<>(end.getMillis(), 40.0)
                ));

        Observable<Void> insertObservable = metricsService.addGaugeData(Observable.just(m1));
        insertObservable.toBlocking().lastOrDefault(null);

        Observable<Double> observable = metricsService
                .findGaugeData(new MetricId(tenantId, GAUGE, "m1"), start.getMillis(), (end.getMillis() + 1000),
                        Aggregate.Min, Aggregate.Max, Aggregate.Average, Aggregate.Sum);
        List<Double> actual = toList(observable);
        List<Double> expected = asList(10.0, 40.0, 25.0, 100.0);

        assertEquals(actual, expected, "The data does not match the expected values");
        assertMetricIndexMatches("t1", GAUGE, singletonList(m1));
    }

    @Test
    public void verifyTTLsSetOnAvailabilityData() throws Exception {
        DateTime start = now().minusMinutes(10);

        metricsService.createTenant(new Tenant("t1")).toBlocking().lastOrDefault(null);
        metricsService.createTenant(
                new Tenant("t2", ImmutableMap.of(AVAILABILITY, 14))).toBlocking().lastOrDefault(null);

        VerifyTTLDataAccess verifyTTLDataAccess = new VerifyTTLDataAccess(dataAccess);

        metricsService.unloadDataRetentions();
        metricsService.loadDataRetentions();
        metricsService.setDataAccess(verifyTTLDataAccess);
        metricsService.setDataAccess(verifyTTLDataAccess);

        List<DataPoint<AvailabilityType>> dataPoints = asList(
                new DataPoint<>(start.getMillis(), UP),
                new DataPoint<>(start.plusMinutes(1).getMillis(), DOWN),
                new DataPoint<>(start.plusMinutes(2).getMillis(), DOWN)
        );
        Metric<AvailabilityType> m1 = new Metric<>(new MetricId("t1", AVAILABILITY, "m1"), dataPoints);

        addAvailabilityDataInThePast(m1, days(2).toStandardDuration());

        Map<String, String> tags = ImmutableMap.of("tag1", "");

        verifyTTLDataAccess.availabilityTagTLLLessThanEqualTo(DEFAULT_TTL - days(2).toStandardSeconds().getSeconds());
        metricsService.tagAvailabilityData(m1, tags, start.getMillis(), start.plusMinutes(2).getMillis()).toBlocking();

        verifyTTLDataAccess.setAvailabilityTTL(days(14).toStandardSeconds().getSeconds());
        Metric<AvailabilityType> m2 = new Metric<>(new MetricId("t2", AVAILABILITY, "m2"),
                singletonList(new DataPoint<>(start.plusMinutes(5).getMillis(), UP)));

        addAvailabilityDataInThePast(m2, days(5).toStandardDuration());

        verifyTTLDataAccess.availabilityTagTLLLessThanEqualTo(days(14).minus(5).toStandardSeconds().getSeconds());
        metricsService.tagAvailabilityData(m2, tags, start.plusMinutes(5).getMillis()).toBlocking();

        metricsService.createTenant(new Tenant("t3", ImmutableMap.of(AVAILABILITY, 24)))
                      .toBlocking()
                      .lastOrDefault(null);
        verifyTTLDataAccess.setAvailabilityTTL(hours(24).toStandardSeconds().getSeconds());
        Metric<AvailabilityType> m3 = new Metric<>(new MetricId("t3", AVAILABILITY, "m3"),
                singletonList(new DataPoint<>(start.getMillis(), UP)));

        metricsService.addAvailabilityData(Observable.just(m3)).toBlocking();
    }

    private void addGaugeDataInThePast(Metric<Double> metric, final Duration duration) throws Exception {
        DataAccess originalDataAccess = metricsService.getDataAccess();
        try {
            metricsService.setDataAccess(new DelegatingDataAccess(dataAccess) {

                @Override
                public Observable<Integer> insertData(Metric<Double> gauge, int ttl) {
                    return Observable.from(insertDataWithNewWriteTime(gauge, ttl))
                            .map(resultSet -> gauge.getDataPoints().size());
                }

                public ResultSetFuture insertDataWithNewWriteTime(Metric<Double> m, int ttl) {
                    int actualTTL = ttl - duration.toStandardSeconds().getSeconds();
                    long writeTime = now().minus(duration).getMillis() * 1000;
                    BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
                    for (DataPoint<Double> d : m.getDataPoints()) {
                        batchStatement.add(insertGaugeDataWithTimestamp.bind(m.getTenantId(), GAUGE.getCode(),
                                m.getId().getName(), m.getId().getInterval().toString(), DPART,
                                getTimeUUID(d.getTimestamp()), d.getValue(), actualTTL, writeTime));
                    }
                    return session.executeAsync(batchStatement);
                }
            });
            metricsService.addGaugeData(Observable.just(metric));
        } finally {
            metricsService.setDataAccess(originalDataAccess);
        }
    }

    private void addAvailabilityDataInThePast(Metric<AvailabilityType> metric, final Duration duration)
            throws Exception {
        DataAccess originalDataAccess = metricsService.getDataAccess();
        try {
            metricsService.setDataAccess(new DelegatingDataAccess(dataAccess) {
                @Override
                public Observable<Integer> insertAvailabilityData(Metric<AvailabilityType> m, int ttl) {
                    int actualTTL = ttl - duration.toStandardSeconds().getSeconds();
                    long writeTime = now().minus(duration).getMillis() * 1000;
                    BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
                    for (DataPoint<AvailabilityType> a : m.getDataPoints()) {
                        batchStatement.add(insertAvailabilityDateWithTimestamp.bind(m.getTenantId(),
                            AVAILABILITY.getCode(), m.getId().getName(), m.getId().getInterval().toString(), DPART,
                            getTimeUUID(a.getTimestamp()), getBytes(a), actualTTL, writeTime));
                    }
                    return rxSession.execute(batchStatement).map(resultSet -> batchStatement.size());
                }
            });
            metricsService.addAvailabilityData(Observable.just(metric));
        } finally {
            metricsService.setDataAccess(originalDataAccess);
        }
    }

    private ByteBuffer getBytes(DataPoint<AvailabilityType> dataPoint) {
        return ByteBuffer.wrap(new byte[]{dataPoint.getValue().getCode()});
    }

    @Test
    public void fetchGaugeDataThatHasTags() throws Exception {
        DateTime end = now();
        DateTime start = end.minusMinutes(10);

        metricsService.createTenant(new Tenant("tenant1")).toBlocking().lastOrDefault(null);

        List<DataPoint<Double>> dataPoints = asList(
                new DataPoint<>(start.getMillis(), 100.0),
                new DataPoint<>(start.plusMinutes(1).getMillis(), 101.1),
                new DataPoint<>(start.plusMinutes(2).getMillis(), 102.2),
                new DataPoint<>(start.plusMinutes(3).getMillis(), 103.3),
                new DataPoint<>(start.plusMinutes(4).getMillis(), 104.4),
                new DataPoint<>(start.plusMinutes(5).getMillis(), 105.5),
                new DataPoint<>(start.plusMinutes(6).getMillis(), 106.6)
        );
        Metric<Double> metric = new Metric<>(new MetricId("tenant1", GAUGE, "m1"), dataPoints);

        metricsService.addGaugeData(Observable.just(metric)).toBlocking().lastOrDefault(null);

        Map<String, String> tags1 = ImmutableMap.of("t1", "1", "t2", "");
        metricsService.tagGaugeData(metric, tags1, start.plusMinutes(2).getMillis()).toBlocking().lastOrDefault(null);

        Map<String, String> tags2 = ImmutableMap.of("t3", "3", "t4", "");
        metricsService.tagGaugeData(metric, tags2, start.plusMinutes(3).getMillis(), start.plusMinutes(5).getMillis()
        ).toBlocking().lastOrDefault(null);

        Observable<DataPoint<Double>> observable = metricsService.findGaugeData(new MetricId("tenant1", GAUGE, "m1"),
                start.getMillis(), end.getMillis());
        List<DataPoint<Double>> actual = ImmutableList.copyOf(observable.toBlocking().toIterable());
        List<DataPoint<Double>> expected = asList(
            new DataPoint<>(start.plusMinutes(6).getMillis(), 106.6),
            new DataPoint<>(start.plusMinutes(5).getMillis(), 105.5),
            new DataPoint<>(start.plusMinutes(4).getMillis(), 104.4),
            new DataPoint<>(start.plusMinutes(3).getMillis(), 103.3),
            new DataPoint<>(start.plusMinutes(2).getMillis(), 102.2),
            new DataPoint<>(start.plusMinutes(1).getMillis(), 101.1),
            new DataPoint<>(start.getMillis(), 100.0)
        );

        assertEquals(actual, expected, "The data does not match the expected values");
        assertEquals(actual.get(3).getTags(), tags2, "The tags do not match");
        assertEquals(actual.get(2).getTags(), tags2, "The tags do not match");
        assertEquals(actual.get(2).getTags(), tags2, "The tags do not match");
        assertEquals(actual.get(4).getTags(), tags1, "The tags do not match");
    }

    @Test
    public void addGaugeDataForMultipleMetrics() throws Exception {
        DateTime start = now().minusMinutes(10);
        DateTime end = start.plusMinutes(8);
        String tenantId = "test-tenant";

        metricsService.createTenant(new Tenant(tenantId)).toBlocking().lastOrDefault(null);

        Metric<Double> m1 = new Metric<>(new MetricId(tenantId, GAUGE, "m1"), asList(
                new DataPoint<>(start.plusSeconds(30).getMillis(), 11.2),
                new DataPoint<>(start.getMillis(), 11.1)
        ));

        Metric<Double> m2 = new Metric<>(new MetricId(tenantId, GAUGE, "m2"), asList(
                new DataPoint<>(start.plusSeconds(30).getMillis(), 12.2),
                new DataPoint<>(start.getMillis(), 12.1)
        ));

        Metric<Double> m3 = new Metric<>(new MetricId(tenantId, GAUGE, "m3"));

        Metric<Double> m4 = new Metric<>(new MetricId(tenantId, GAUGE, "m4"), emptyMap(), 24, asList(
                new DataPoint<>(start.plusSeconds(30).getMillis(), 55.5),
                new DataPoint<>(end.getMillis(), 66.6)
        ));
        metricsService.createMetric(m4).toBlocking().lastOrDefault(null);

        metricsService.addGaugeData(Observable.just(m1, m2, m3, m4)).toBlocking().lastOrDefault(null);

        Observable<DataPoint<Double>> observable1 = metricsService.findGaugeData(m1.getId(),
                start.getMillis(), end.getMillis());
        assertEquals(toList(observable1), m1.getDataPoints(), "The gauge data for " + m1.getId() + " does not match");

        Observable<DataPoint<Double>> observable2 = metricsService.findGaugeData(m2.getId(),
                start.getMillis(), end.getMillis());
        assertEquals(toList(observable2), m2.getDataPoints(), "The gauge data for " + m2.getId() + " does not match");

        Observable<DataPoint<Double>> observable3 = metricsService.findGaugeData(m3.getId(),
                start.getMillis(), end.getMillis());
        assertTrue(toList(observable3).isEmpty(), "Did not expect to get back results for " + m3.getId());

        Observable<DataPoint<Double>> observable4 = metricsService.findGaugeData(m4.getId(),
                start.getMillis(), end.getMillis());
        Metric<Double> expected = new Metric<>(new MetricId(tenantId, GAUGE, "m4"), emptyMap(), 24,
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

        Metric<AvailabilityType> m1 = new Metric<>(new MetricId(tenantId, AVAILABILITY, "m1"), asList(
                new DataPoint<>(start.plusSeconds(10).getMillis(), UP),
                new DataPoint<>(start.plusSeconds(20).getMillis(), DOWN)
        ));
        Metric<AvailabilityType> m2 = new Metric<>(new MetricId(tenantId, AVAILABILITY, "m2"), asList(
                new DataPoint<>(start.plusSeconds(15).getMillis(), DOWN),
                new DataPoint<>(start.plusSeconds(30).getMillis(), UP)
        ));
        Metric<AvailabilityType> m3 = new Metric<>(new MetricId(tenantId, AVAILABILITY, "m3"));

        metricsService.addAvailabilityData(Observable.just(m1, m2, m3)).toBlocking().lastOrDefault(null);

        List<DataPoint<AvailabilityType>> actual = metricsService.findAvailabilityData(m1.getId(),
                start.getMillis(), end.getMillis()).toList().toBlocking().last();
        assertEquals(actual, m1.getDataPoints(), "The availability data does not match expected values");

        actual = metricsService.findAvailabilityData(m2.getId(), start.getMillis(), end.getMillis()).toList()
                .toBlocking().last();
        assertEquals(actual, m2.getDataPoints(), "The availability data does not match expected values");

        actual = metricsService.findAvailabilityData(m3.getId(), start.getMillis(), end.getMillis()).toList()
            .toBlocking().last();
        assertEquals(actual.size(), 0, "Did not expect to get back results since there is no data for " + m3);

        Metric<AvailabilityType> m4 = new Metric<>(new MetricId(tenantId, AVAILABILITY, "m4"), emptyMap(), 24, asList(
                new DataPoint<>(start.plusMinutes(2).getMillis(), UP),
                new DataPoint<>(end.plusMinutes(2).getMillis(), UP)));
        metricsService.createMetric(m4).toBlocking().lastOrDefault(null);

        metricsService.addAvailabilityData(Observable.just(m4)).toBlocking().lastOrDefault(null);

        actual = metricsService.findAvailabilityData(m4.getId(), start.getMillis(), end.getMillis()).toList()
            .toBlocking().last();
        Metric<AvailabilityType> expected = new Metric<>(m4.getId(), emptyMap(), 24,
                singletonList(new DataPoint<>(start.plusMinutes(2).getMillis(), UP)));
        assertEquals(actual, expected.getDataPoints(), "The availability data does not match expected values");

        assertMetricIndexMatches(tenantId, AVAILABILITY, asList(m1, m2, m3, m4));
    }

    @Test
    public void fetchAvailabilityDataThatHasTags() throws Exception {
        DateTime end = now();
        DateTime start = end.minusMinutes(10);

        metricsService.createTenant(new Tenant("tenant1")).toBlocking().lastOrDefault(null);

        List<DataPoint<AvailabilityType>> dataPoints = asList(
                new DataPoint<>(start.getMillis(), UP),
                new DataPoint<>(start.plusMinutes(1).getMillis(), DOWN),
                new DataPoint<>(start.plusMinutes(2).getMillis(), DOWN),
                new DataPoint<>(start.plusMinutes(3).getMillis(), UP),
                new DataPoint<>(start.plusMinutes(4).getMillis(), DOWN),
                new DataPoint<>(start.plusMinutes(4).getMillis(), DOWN),
                new DataPoint<>(start.plusMinutes(5).getMillis(), UP),
                new DataPoint<>(start.plusMinutes(6).getMillis(), UP)
        );
        Metric<AvailabilityType> metric = new Metric<>(new MetricId("tenant1", AVAILABILITY, "A1"), dataPoints);

        metricsService.addAvailabilityData(Observable.just(metric)).toBlocking().lastOrDefault(null);

        Map<String, String> tags1 = ImmutableMap.of("t1", "1", "t2", "");
        metricsService.tagAvailabilityData(metric, tags1, start.plusMinutes(2).getMillis()).toBlocking()
                .lastOrDefault(null);

        Map<String, String> tags2 = ImmutableMap.of("t3", "3", "t4", "");
        metricsService.tagAvailabilityData(metric, tags2, start.plusMinutes(3).getMillis(),
            start.plusMinutes(5).getMillis()).toBlocking().lastOrDefault(null);

        List<DataPoint<AvailabilityType>> actual = metricsService.findAvailabilityData(
                metric.getId(), start.getMillis(), end.getMillis()).toList().toBlocking().last();
        List<DataPoint<AvailabilityType>> expected = asList(
            new DataPoint<>(start.getMillis(), UP),
            new DataPoint<>(start.plusMinutes(1).getMillis(), DOWN),
            new DataPoint<>(start.plusMinutes(2).getMillis(), DOWN),
            new DataPoint<>(start.plusMinutes(3).getMillis(), UP),
            new DataPoint<>(start.plusMinutes(4).getMillis(), DOWN),
            new DataPoint<>(start.plusMinutes(5).getMillis(), UP),
            new DataPoint<>(start.plusMinutes(6).getMillis(), UP)
        );

        assertEquals(actual, expected, "The data does not match the expected values");
        assertEquals(actual.get(3).getTags(), tags2, "The tags do not match");
        assertEquals(actual.get(2).getTags(), tags1, "The tags do not match");
        assertEquals(actual.get(2).getTags(), tags1, "The tags do not match");
        assertEquals(actual.get(4).getTags(), tags2, "The tags do not match");
    }

    @Test
    public void findDistinctAvailabilities() throws Exception {
        DateTime end = now();
        DateTime start = end.minusMinutes(20);
        String tenantId = "tenant1";
        MetricId metricId = new MetricId("tenant1", AVAILABILITY, "A1");

        metricsService.createTenant(new Tenant(tenantId)).toBlocking().lastOrDefault(null);

        Metric<AvailabilityType> metric = new Metric<>(metricId, asList(
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

        metricsService.addAvailabilityData(Observable.just(metric)).toBlocking().lastOrDefault(null);

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
    public void tagGaugeDataByDateRangeAndQueryByMultipleTags() throws Exception {
        String tenant = "tag-test";
        DateTime start = now().minusMinutes(20);

        metricsService.createTenant(new Tenant(tenant)).toBlocking().lastOrDefault(null);

        DataPoint<Double> d1 = new DataPoint<>(start.getMillis(), 101.1);
        DataPoint<Double> d2 = new DataPoint<>(start.plusMinutes(2).getMillis(), 101.2);
        DataPoint<Double> d3 = new DataPoint<>(start.plusMinutes(6).getMillis(), 102.2);
        DataPoint<Double> d4 = new DataPoint<>(start.plusMinutes(8).getMillis(), 102.3);
        DataPoint<Double> d5 = new DataPoint<>(start.plusMinutes(4).getMillis(), 102.1);
        DataPoint<Double> d6 = new DataPoint<>(start.plusMinutes(4).getMillis(), 101.4);
        DataPoint<Double> d7 = new DataPoint<>(start.plusMinutes(10).getMillis(), 102.4);
        DataPoint<Double> d8 = new DataPoint<>(start.plusMinutes(6).getMillis(), 103.1);
        DataPoint<Double> d9 = new DataPoint<>(start.plusMinutes(7).getMillis(), 103.1);

        Metric<Double> m1 = new Metric<>(new MetricId(tenant, GAUGE, "m1"), asList(d1, d2, d6));

        Metric<Double> m2 = new Metric<>(new MetricId(tenant, GAUGE, "m2"), asList(d3, d4, d5, d7));

        Metric<Double> m3 = new Metric<>(new MetricId(tenant, GAUGE, "m3"), asList(d8, d9));

        metricsService.addGaugeData(Observable.just(m1, m2, m3)).toBlocking().lastOrDefault(null);

        Map<String, String> tags1 = ImmutableMap.of("t1", "1");
        Map<String, String> tags2 = ImmutableMap.of("t2", "2");

        metricsService.tagGaugeData(m1, tags1, start.getMillis(), start.plusMinutes(6).getMillis()).toBlocking()
                .lastOrDefault(null);
        metricsService.tagGaugeData(m2, tags1, start.getMillis(), start.plusMinutes(6).getMillis()).toBlocking()
                .lastOrDefault(null);
        metricsService.tagGaugeData(m1, tags2, start.plusMinutes(4).getMillis(), start.plusMinutes(8).getMillis())
        .toBlocking().lastOrDefault(null);
        metricsService.tagGaugeData(m2, tags2, start.plusMinutes(4).getMillis(), start.plusMinutes(8).getMillis())
        .toBlocking().lastOrDefault(null);
        metricsService.tagGaugeData(m3, tags2, start.plusMinutes(4).getMillis(), start.plusMinutes(8).getMillis())
        .toBlocking().lastOrDefault(null);

        Map<MetricId, Set<DataPoint<Double>>> actual = metricsService.findGaugeDataByTags(tenant,
                ImmutableMap.of("t1", "1", "t2", "2")).toBlocking().toIterable().iterator().next();
        ImmutableMap<MetricId, ImmutableSet<DataPoint<Double>>> expected = ImmutableMap.of(
                new MetricId(tenant, GAUGE, "m1"), ImmutableSet.of(d1, d2, d6),
                new MetricId(tenant, GAUGE, "m2"), ImmutableSet.of(d5, d3)
        );

        assertEquals(actual, expected, "The tagged data does not match");
    }

    @Test
    public void tagAvailabilityByDateRangeAndQueryByMultipleTags() throws Exception {
        String tenant = "tag-test";
        DateTime start = now().minusMinutes(20);

        metricsService.createTenant(new Tenant(tenant)).toBlocking().lastOrDefault(null);

        DataPoint<AvailabilityType> a1 = new DataPoint<>(start.getMillis(), UP);
        DataPoint<AvailabilityType> a2 = new DataPoint<>(start.plusMinutes(2).getMillis(), UP);
        DataPoint<AvailabilityType> a3 = new DataPoint<>(start.plusMinutes(6).getMillis(), DOWN);
        DataPoint<AvailabilityType> a4 = new DataPoint<>(start.plusMinutes(8).getMillis(), DOWN);
        DataPoint<AvailabilityType> a5 = new DataPoint<>(start.plusMinutes(4).getMillis(), UP);
        DataPoint<AvailabilityType> a6 = new DataPoint<>(start.plusMinutes(4).getMillis(), DOWN);
        DataPoint<AvailabilityType> a7 = new DataPoint<>(start.plusMinutes(10).getMillis(), UP);
        DataPoint<AvailabilityType> a8 = new DataPoint<>(start.plusMinutes(6).getMillis(), DOWN);
        DataPoint<AvailabilityType> a9 = new DataPoint<>(start.plusMinutes(7).getMillis(), UP);

        Metric<AvailabilityType> m1 = new Metric<>(new MetricId(tenant, AVAILABILITY, "m1"), asList(a1, a2, a6));
        Metric<AvailabilityType> m2 = new Metric<>(new MetricId(tenant, AVAILABILITY, "m2"),
                asList(a3, a4, a5, a7));
        Metric<AvailabilityType> m3 = new Metric<>(new MetricId(tenant, AVAILABILITY, "m3"), asList(a8, a9));

        metricsService.addAvailabilityData(Observable.just(m1, m2, m3)).toBlocking().lastOrDefault(null);

        Map<String, String> tags1 = ImmutableMap.of("t1", "1");
        Map<String, String> tags2 = ImmutableMap.of("t2", "2");

        metricsService.tagAvailabilityData(m1, tags1, start.getMillis(), start.plusMinutes(6).getMillis()).toBlocking
                ().lastOrDefault(null);
        metricsService.tagAvailabilityData(m2, tags1, start.getMillis(), start.plusMinutes(6).getMillis()).toBlocking
                ().lastOrDefault(null);
        metricsService.tagAvailabilityData(m1, tags2, start.plusMinutes(4).getMillis(),
            start.plusMinutes(8).getMillis()).toBlocking().lastOrDefault(null);
        metricsService.tagAvailabilityData(m2, tags2, start.plusMinutes(4).getMillis(),
            start.plusMinutes(8).getMillis()).toBlocking().lastOrDefault(null);
        metricsService.tagAvailabilityData(m3, tags2, start.plusMinutes(4).getMillis(),
            start.plusMinutes(8).getMillis()).toBlocking().lastOrDefault(null);

        Map<MetricId, Set<DataPoint<AvailabilityType>>> actual = metricsService
            .findAvailabilityByTags(tenant, ImmutableMap.of("t1", "1", "t2", "2")).toBlocking().toIterable().iterator
                        ().next();
        ImmutableMap<MetricId, ImmutableSet<DataPoint<AvailabilityType>>> expected = ImmutableMap.of(
                new MetricId(tenant, AVAILABILITY, "m1"), ImmutableSet.of(a1, a2, a6),
                new MetricId(tenant, AVAILABILITY, "m2"), ImmutableSet.of(a5, a3)
        );

        assertEquals(actual, expected, "The tagged data does not match");
    }

    @Test
    public void tagIndividualGaugeDataPoints() throws Exception {
        String tenant = "tag-test";
        DateTime start = now().minusMinutes(20);

        metricsService.createTenant(new Tenant(tenant)).toBlocking().lastOrDefault(null);

        DataPoint<Double> d1 = new DataPoint<>(start.getMillis(), 101.1);
        DataPoint<Double> d2 = new DataPoint<>(start.plusMinutes(2).getMillis(), 101.2);
        DataPoint<Double> d3 = new DataPoint<>(start.plusMinutes(6).getMillis(), 102.2);
        DataPoint<Double> d4 = new DataPoint<>(start.plusMinutes(8).getMillis(), 102.3);
        DataPoint<Double> d5 = new DataPoint<>(start.plusMinutes(4).getMillis(), 102.1);
        DataPoint<Double> d6 = new DataPoint<>(start.plusMinutes(4).getMillis(), 101.4);
        DataPoint<Double> d7 = new DataPoint<>(start.plusMinutes(10).getMillis(), 102.4);
        DataPoint<Double> d8 = new DataPoint<>(start.plusMinutes(6).getMillis(), 103.1);
        DataPoint<Double> d9 = new DataPoint<>(start.plusMinutes(7).getMillis(), 103.1);

        Metric<Double> m1 = new Metric<>(new MetricId(tenant, GAUGE, "m1"), asList(d1, d2, d6));

        Metric<Double> m2 = new Metric<>(new MetricId(tenant, GAUGE, "m2"), asList(d3, d4, d5, d7));

        Metric<Double> m3 = new Metric<>(new MetricId(tenant, GAUGE, "m3"), asList(d8, d9));

        metricsService.addGaugeData(Observable.just(m1, m2, m3)).toBlocking().lastOrDefault(null);

        Map<String, String> tags1 = ImmutableMap.of("t1", "");
        metricsService.tagGaugeData(m1, tags1, d1.getTimestamp()).toBlocking().lastOrDefault(null);
//        assertTrue(tagFuture.wasApplied(), "Tagging " + d1 + " returned unexpected results");

        Map<String, String> tags2 = ImmutableMap.of("t1", "", "t2", "", "t3", "");
        metricsService.tagGaugeData(m1, tags2, d2.getTimestamp()).toBlocking().lastOrDefault(null);
//        assertTrue(tagFuture.wasApplied(), "Tagging " + d2 + " returned unexpected results");

//        tagFuture = metricsService.tagGaugeData(m1, tags1, start.minusMinutes(10).getMillis());
//        assertEquals(getUninterruptibly(tagFuture), Collections.emptyList(),
//                "No data should be returned since there is no data for this time");

        Map<String, String> tags3 = ImmutableMap.of("t1", "", "t2", "");
        metricsService.tagGaugeData(m2, tags3, d3.getTimestamp()).toBlocking().lastOrDefault(null);
//        assertTrue(tagFuture.wasApplied(), "Tagging " + d3 + " returned unexpected results");

        Map<String, String> tags4 = ImmutableMap.of("t3", "", "t4", "");
        metricsService.tagGaugeData(m2, tags4, d4.getTimestamp()).toBlocking().lastOrDefault(null);
//        assertTrue(tagFuture.wasApplied(), "Tagging " + d4 + " returned unexpected results");

        Map<MetricId, Set<DataPoint<Double>>> actual = metricsService.findGaugeDataByTags(tenant,
                ImmutableMap.of("t2", "", "t3", "")).toBlocking().lastOrDefault(null);

        ImmutableMap<MetricId, ImmutableSet<DataPoint<Double>>> expected = ImmutableMap.of(
                new MetricId(tenant, GAUGE, "m1"), ImmutableSet.of(d2),
                new MetricId(tenant, GAUGE, "m2"), ImmutableSet.of(d3, d4)
        );

        assertEquals(actual, expected, "The tagged data does not match");
    }

    @Test
    public void tagIndividualAvailabilityDataPoints() throws Exception {
        String tenant = "tag-test";
        DateTime start = now().minusMinutes(20);

        metricsService.createTenant(new Tenant(tenant)).toBlocking().lastOrDefault(null);

        DataPoint<AvailabilityType> a1 = new DataPoint<>(start.getMillis(), UP);
        DataPoint<AvailabilityType> a2 = new DataPoint<>(start.plusMinutes(2).getMillis(), UP);
        DataPoint<AvailabilityType> a3 = new DataPoint<>(start.plusMinutes(6).getMillis(), DOWN);
        DataPoint<AvailabilityType> a4 = new DataPoint<>(start.plusMinutes(8).getMillis(), DOWN);
        DataPoint<AvailabilityType> a5 = new DataPoint<>(start.plusMinutes(4).getMillis(), UP);
        DataPoint<AvailabilityType> a6 = new DataPoint<>(start.plusMinutes(4).getMillis(), DOWN);
        DataPoint<AvailabilityType> a7 = new DataPoint<>(start.plusMinutes(10).getMillis(), UP);
        DataPoint<AvailabilityType> a8 = new DataPoint<>(start.plusMinutes(6).getMillis(), DOWN);
        DataPoint<AvailabilityType> a9 = new DataPoint<>(start.plusMinutes(7).getMillis(), UP);

        Metric<AvailabilityType> m1 = new Metric<>(new MetricId(tenant, AVAILABILITY, "m1"), asList(a1, a2, a6));
        Metric<AvailabilityType> m2 = new Metric<>(new MetricId(tenant, AVAILABILITY, "m2"),
                asList(a3, a4, a5, a7));
        Metric<AvailabilityType> m3 = new Metric<>(new MetricId(tenant, AVAILABILITY, "m3"), asList(a8, a9));

        metricsService.addAvailabilityData(Observable.just(m1, m2, m3)).toBlocking().lastOrDefault(null);

        Map<String, String> tags1 = ImmutableMap.of("t1", "");
        metricsService.tagAvailabilityData(m1, tags1, a1.getTimestamp()).toBlocking().lastOrDefault(null);
//        assertTrue(tagFuture.wasApplied(), "Tagging " + a1 + " returned unexpected results");

        Map<String, String> tags2 = ImmutableMap.of("t1", "", "t2", "", "t3", "");
        metricsService.tagAvailabilityData(m1, tags2, a2.getTimestamp()).toBlocking().lastOrDefault(null);
//        assertTrue(tagFuture.wasApplied(), "Tagging " + a2 + " returned unexpected results");

//        tagFuture = metricsService.tagAvailabilityData(m1, tags1, start.minusMinutes(10).getMillis());
//        assertEquals(getUninterruptibly(tagFuture), Collections.emptyList(),
//            "No data should be returned since there is no data for this time");

        Map<String, String> tags3 = ImmutableMap.of("t2", "", "t3", "");
        metricsService.tagAvailabilityData(m2, tags3, a3.getTimestamp()).toBlocking().lastOrDefault(null);
//        assertTrue(tagFuture.wasApplied(), "Tagging " + a3 + " returned unexpected results");

        Map<String, String> tags4 = ImmutableMap.of("t3", "", "t4", "");
        metricsService.tagAvailabilityData(m2, tags4, a4.getTimestamp()).toBlocking().lastOrDefault(null);
//        assertTrue(tagFuture.wasApplied(), "Tagging " + a4 + " returned unexpected results");

        Map<MetricId, Set<DataPoint<AvailabilityType>>> actual = metricsService.findAvailabilityByTags(
                tenant, tags3).toBlocking().last();
        ImmutableMap<MetricId, ImmutableSet<DataPoint<AvailabilityType>>> expected = ImmutableMap.of(
            new MetricId(tenant, AVAILABILITY, "m1"), ImmutableSet.of(a2),
            new MetricId(tenant, AVAILABILITY, "m2"), ImmutableSet.of(a3, a4)
        );

        assertEquals(actual, expected, "The tagged data does not match");
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
        Metric<Double> m1 = new Metric<>(new MetricId(tenantId, GAUGE, "m1"), dataPoints);

        metricsService.addGaugeData(Observable.just(m1)).toBlocking().lastOrDefault(null);

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

    private <T> List<T> toList(Observable<T> observable) {
        return ImmutableList.copyOf(observable.toBlocking().toIterable());
    }

    private void assertMetricIndexMatches(String tenantId, MetricType type, List<Metric> expected)
        throws Exception {
        List<Metric> actualIndex = ImmutableList.copyOf(metricsService.findMetrics(tenantId, type).toBlocking()
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
        MetricType type;
        MetricId id;

        public MetricsTagsIndexEntry(String tagValue, MetricType type, MetricId id) {
            this.tagValue = tagValue;
            this.type = type;
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MetricsTagsIndexEntry that = (MetricsTagsIndexEntry) o;

            if (!id.equals(that.id)) return false;
            if (!tagValue.equals(that.tagValue)) return false;
            if (type != that.type) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = tagValue.hashCode();
            result = 31 * result + type.hashCode();
            result = 31 * result + id.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return com.google.common.base.Objects.toStringHelper(this)
                .add("tagValue", tagValue)
                .add("type", type)
                .add("id", id)
                .toString();
        }
    }

    private void assertMetricsTagsIndexMatches(String tenantId, String tag, List<MetricsTagsIndexEntry> expected)
        throws Exception {
        ResultSet resultSet = dataAccess.findMetricsByTagName(tenantId, tag).toBlocking().first();
        List<MetricsTagsIndexEntry> actual = new ArrayList<>();

        for (Row row : resultSet) {
            MetricType type = MetricType.fromCode(row.getInt(0));
            MetricId id = new MetricId(tenantId, type, row.getString(1), Interval.parse(row.getString(2)));
            actual.add(new MetricsTagsIndexEntry(row.getString(3), type, id)); // Need value here.. pff.
        }

        assertEquals(actual, expected, "The metrics tags index entries do not match");
    }

    private void assertDataRetentionsIndexMatches(String tenantId, MetricType type, Set<Retention> expected)
        throws Exception {
        ResultSetFuture queryFuture = dataAccess.findDataRetentions(tenantId, type);
        ListenableFuture<Set<Retention>> retentionsFuture = Futures.transform(queryFuture,
                new DataRetentionsMapper(tenantId, type));
        Set<Retention> actual = getUninterruptibly(retentionsFuture);

        assertEquals(actual, expected, "The data retentions are wrong");
    }

    private static class VerifyTTLDataAccess extends DelegatingDataAccess {

        private int gaugeTTL;

        private int gaugeTagTTL;

        private int availabilityTTL;

        private int availabilityTagTTL;

        public VerifyTTLDataAccess(DataAccess instance) {
            super(instance);
            gaugeTTL = DEFAULT_TTL;
            gaugeTagTTL = DEFAULT_TTL;
            availabilityTTL = DEFAULT_TTL;
            availabilityTagTTL = DEFAULT_TTL;
        }

        public void setGaugeTTL(int expectedTTL) {
            this.gaugeTTL = expectedTTL;
        }

        public void gaugeTagTTLLessThanEqualTo(int gaugeTagTTL) {
            this.gaugeTagTTL = gaugeTagTTL;
        }

        public void setAvailabilityTTL(int availabilityTTL) {
            this.availabilityTTL = availabilityTTL;
        }

        public void availabilityTagTLLLessThanEqualTo(int availabilityTagTTL) {
            this.availabilityTagTTL = availabilityTagTTL;
        }

        @Override
        public Observable<Integer> insertData(Metric<Double> gauge, int ttl) {
            assertEquals(ttl, gaugeTTL, "The gauge TTL does not match the expected value when inserting data");
            return super.insertData(gauge, ttl);
        }

        @Override
        public Observable<Integer> insertAvailabilityData(Metric<AvailabilityType> metric, int ttl) {
            assertEquals(ttl, availabilityTTL, "The availability data TTL does not match the expected value when " +
                "inserting data");
            return super.insertAvailabilityData(metric, ttl);
        }

        @Override
        public Observable<ResultSet> insertGaugeTag(String tag, String tagValue, Metric<Double> metric,
                Observable<TTLDataPoint<Double>> data) {

            List<TTLDataPoint<Double>> first = data.toList().toBlocking().first();

            for (TTLDataPoint<Double> d : first) {
                assertTrue(d.getTTL() <= gaugeTagTTL, "Expected the TTL to be <= " + gaugeTagTTL +
                    " but it was " + d.getTTL());
            }
            return super.insertGaugeTag(tag, tagValue, metric, data);
        }

        @Override
        public Observable<ResultSet> insertAvailabilityTag(String tag, String tagValue,
                Metric<AvailabilityType> metric, Observable<TTLDataPoint<AvailabilityType>> data) {
            List<TTLDataPoint<AvailabilityType>> first = data.toList().toBlocking().first();
            for (TTLDataPoint<AvailabilityType> a : first) {
                assertTrue(a.getTTL() <= availabilityTagTTL, "Expected the TTL to be <= " + availabilityTagTTL +
                    " but it was " + a.getTTL());
            }
            return super.insertAvailabilityTag(tag, tagValue, metric, data);
        }
    }

}
