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
package org.hawkular.metrics.core.impl.cassandra;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.hawkular.metrics.core.api.AvailabilityType.DOWN;
import static org.hawkular.metrics.core.api.AvailabilityType.UNKNOWN;
import static org.hawkular.metrics.core.api.AvailabilityType.UP;
import static org.hawkular.metrics.core.api.Metric.DPART;
import static org.hawkular.metrics.core.api.MetricType.AVAILABILITY;
import static org.hawkular.metrics.core.api.MetricType.GAUGE;
import static org.hawkular.metrics.core.impl.cassandra.MetricsServiceCassandra.DEFAULT_TTL;
import static org.joda.time.DateTime.now;
import static org.joda.time.Days.days;
import static org.joda.time.Hours.hours;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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

import org.hawkular.metrics.core.api.Availability;
import org.hawkular.metrics.core.api.AvailabilityData;
import org.hawkular.metrics.core.api.Gauge;
import org.hawkular.metrics.core.api.GaugeData;
import org.hawkular.metrics.core.api.Interval;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricAlreadyExistsException;
import org.hawkular.metrics.core.api.MetricData;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.Retention;
import org.hawkular.metrics.core.api.Tenant;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import rx.Observable;

/**
 * @author John Sanda
 */
public class MetricsServiceCassandraITest extends MetricsITest {

    private MetricsServiceCassandra metricsService;

    private DataAccess dataAccess;

    private PreparedStatement insertGaugeDataWithTimestamp;

    private PreparedStatement insertAvailabilityDateWithTimestamp;

    @BeforeClass
    public void initClass() {
        initSession();
        metricsService = new MetricsServiceCassandra();
        metricsService.startUp(session);
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
        Tenant t1 = new Tenant().setId("t1").setRetention(GAUGE, 24).setRetention(AVAILABILITY, 24);
        Tenant t2 = new Tenant().setId("t2").setRetention(GAUGE, 72);
        Tenant t3 = new Tenant().setId("t3").setRetention(AVAILABILITY, 48);
        Tenant t4 = new Tenant().setId("t4");

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
                new MetricId("[" + GAUGE.getText() + "]"), hours(24).toStandardSeconds().getSeconds())));
        assertDataRetentionsIndexMatches(t1.getId(), AVAILABILITY, ImmutableSet.of(new Retention(
                new MetricId("[" + AVAILABILITY.getText() + "]"), hours(24).toStandardSeconds().getSeconds())));
    }

    @Test
    public void createAndFindMetrics() throws Exception {
        Optional<? extends Metric<? extends MetricData>> result = metricsService.findMetric("t1", GAUGE,
                new MetricId("does-not-exist")).toBlocking().last();
        assertNotNull(result, "null should not be returned when metric is not found");
        assertFalse(result.isPresent(), "Did not expect a value when the metric is not found");


        Gauge m1 = new Gauge("t1", new MetricId("m1"), ImmutableMap.of("a1", "1", "a2", "2"),
            24);
        metricsService.createMetric(m1).toBlocking().lastOrDefault(null);

        Gauge actual = (Gauge) metricsService.findMetric(m1.getTenantId(), m1.getType(), m1.getId())
                .toBlocking().last().get();
        assertEquals(actual, m1, "The metric does not match the expected value");

        Availability m2 = new Availability("t1", new MetricId("m2"), ImmutableMap.of("a3", "3", "a4", "3"));
        metricsService.createMetric(m2).toBlocking().lastOrDefault(null);

        Availability actualAvail = (Availability) metricsService.findMetric(m2.getTenantId(), m2.getType(), m2.getId())
                .toBlocking().last().get();
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

        Gauge m3 = new Gauge("t1", new MetricId("m3"));
        m3.setDataRetention(24);
        metricsService.createMetric(m3).toBlocking().lastOrDefault(null);

        Gauge m4 = new Gauge("t1", new MetricId("m4"), ImmutableMap.of("a1", "A", "a2", ""));
        metricsService.createMetric(m4).toBlocking().lastOrDefault(null);

        assertMetricIndexMatches("t1", GAUGE, asList(m1, m3, m4));
        assertMetricIndexMatches("t1", AVAILABILITY, singletonList(m2));

        assertDataRetentionsIndexMatches("t1", GAUGE, ImmutableSet.of(new Retention(m3.getId(), 24),
                new Retention(m1.getId(), 24)));

        assertMetricsTagsIndexMatches("t1", "a1", asList(
                new MetricsTagsIndexEntry("1", GAUGE, m1.getId()),
                new MetricsTagsIndexEntry("A", GAUGE, m4.getId())
        ));
    }

    @Test
    public void updateMetricTags() throws Exception {
        Gauge metric = new Gauge("t1", new MetricId("m1"), ImmutableMap.of("a1", "1", "a2", "2"));
        metricsService.createMetric(metric).toBlocking().lastOrDefault(null);

        Map<String, String> additions = ImmutableMap.of("a2", "two", "a3", "3");
        metricsService.addTags(metric, additions).toBlocking().lastOrDefault
                (null);

        Map<String, String> deletions = ImmutableMap.of("a1", "1");
        metricsService.deleteTags(metric, deletions).toBlocking()
                .lastOrDefault(null);

        Metric<? extends MetricData> updatedMetric = metricsService.findMetric(metric.getTenantId(), GAUGE,
            metric.getId()).toBlocking().last().get();

        assertEquals(updatedMetric.getTags(), ImmutableMap.of("a2", "two", "a3", "3"),
            "The updated meta data does not match the expected values");

        assertMetricIndexMatches(metric.getTenantId(), GAUGE, singletonList(updatedMetric));
    }

    @Test
    public void addAndFetchGaugeData() throws Exception {
        DateTime start = now().minusMinutes(30);
        DateTime end = start.plusMinutes(20);

        metricsService.createTenant(new Tenant().setId("t1")).toBlocking().lastOrDefault(null);

        Gauge m1 = new Gauge("t1", new MetricId("m1"));
//        m1.setDataRetention(DEFAULT_TTL);
        m1.addData(new GaugeData(start.getMillis(), 1.1));
        m1.addData(new GaugeData(start.plusMinutes(2).getMillis(), 2.2));
        m1.addData(new GaugeData(start.plusMinutes(4).getMillis(), 3.3));
        m1.addData(new GaugeData(end.getMillis(), 4.4));

        Observable<Void> insertObservable = metricsService.addGaugeData(Observable.just(m1));
        insertObservable.toBlocking().lastOrDefault(null);

        Observable<GaugeData> observable = metricsService.findGaugeData("t1", new MetricId("m1"),
                start.getMillis(), end.getMillis());
        List<GaugeData> actual = toList(observable);
        List<GaugeData> expected = asList(
                new GaugeData(start.plusMinutes(4).getMillis(), 3.3),
                new GaugeData(start.plusMinutes(2).getMillis(), 2.2),
                new GaugeData(start.getMillis(), 1.1)
        );

        assertEquals(actual, expected, "The data does not match the expected values");
        assertMetricIndexMatches("t1", GAUGE, singletonList(m1));
    }

    @Test
    public void verifyTTLsSetOnGaugeData() throws Exception {
        DateTime start = now().minusMinutes(10);

        metricsService.createTenant(new Tenant().setId("t1")).toBlocking().lastOrDefault(null);
        metricsService.createTenant(new Tenant().setId("t2").setRetention(GAUGE, days(14).toStandardHours().getHours()))
                      .toBlocking()
                      .lastOrDefault(null);

        VerifyTTLDataAccess verifyTTLDataAccess = new VerifyTTLDataAccess(dataAccess);

        metricsService.unloadDataRetentions();
        metricsService.loadDataRetentions();
        metricsService.setDataAccess(verifyTTLDataAccess);

        Gauge m1 = new Gauge("t1", new MetricId("m1"));
        m1.addData(start.getMillis(), 1.01);
        m1.addData(start.plusMinutes(1).getMillis(), 1.02);
        m1.addData(start.plusMinutes(2).getMillis(), 1.03);

        addDataInThePast(m1, days(2).toStandardDuration());

        Map<String, String> tags = ImmutableMap.of("tag1", "");

        verifyTTLDataAccess.gaugeTagTTLLessThanEqualTo(DEFAULT_TTL - days(2).toStandardSeconds().getSeconds());
        metricsService.tagGaugeData(m1, tags, start.getMillis(),
            start.plusMinutes(2).getMillis()).toBlocking();

        verifyTTLDataAccess.setGaugeTTL(days(14).toStandardSeconds().getSeconds());
        Gauge m2 = new Gauge("t2", new MetricId("m2"));
        m2.addData(start.plusMinutes(5).getMillis(), 2.02);
        addDataInThePast(m2, days(3).toStandardDuration());

        verifyTTLDataAccess.gaugeTagTTLLessThanEqualTo(days(14).minus(3).toStandardSeconds().getSeconds());
        metricsService.tagGaugeData(m2, tags, start.plusMinutes(5).getMillis()).toBlocking().last();

        metricsService.createTenant(new Tenant().setId("t3").setRetention(GAUGE, 24))
                      .toBlocking()
                      .lastOrDefault(null);
        verifyTTLDataAccess.setGaugeTTL(hours(24).toStandardSeconds().getSeconds());
        Gauge m3 = new Gauge("t3", new MetricId("m3"));
        m3.addData(start.getMillis(), 3.03);
        metricsService.addGaugeData(Observable.just(m3)).toBlocking().lastOrDefault(null);

        Gauge m4 = new Gauge("t2", new MetricId("m4"), emptyMap(), 28);
        metricsService.createMetric(m4).toBlocking().lastOrDefault(null);

        verifyTTLDataAccess.setGaugeTTL(28);
        m4.addData(start.plusMinutes(3).getMillis(), 4.1);
        m4.addData(start.plusMinutes(4).getMillis(), 4.2);
        metricsService.addGaugeData(Observable.just(m4)).toBlocking().lastOrDefault(null);
    }

    @Test
    public void verifyTTLsSetOnAvailabilityData() throws Exception {
        DateTime start = now().minusMinutes(10);

        metricsService.createTenant(new Tenant().setId("t1")).toBlocking().lastOrDefault(null);
        metricsService.createTenant(
                new Tenant().setId("t2").setRetention(AVAILABILITY, days(14).toStandardHours().getHours())
        ).toBlocking().lastOrDefault(null);

        VerifyTTLDataAccess verifyTTLDataAccess = new VerifyTTLDataAccess(dataAccess);

        metricsService.unloadDataRetentions();
        metricsService.loadDataRetentions();
        metricsService.setDataAccess(verifyTTLDataAccess);
        metricsService.setDataAccess(verifyTTLDataAccess);

        Availability m1 = new Availability("t1", new MetricId("m1"));
        m1.addData(new AvailabilityData(start.getMillis(), UP));
        m1.addData(new AvailabilityData(start.plusMinutes(1).getMillis(), DOWN));
        m1.addData(new AvailabilityData(start.plusMinutes(2).getMillis(), DOWN));
        addDataInThePast(m1, days(2).toStandardDuration());

        Map<String, String> tags = ImmutableMap.of("tag1", "");

        verifyTTLDataAccess.availabilityTagTLLLessThanEqualTo(DEFAULT_TTL - days(2).toStandardSeconds().getSeconds());
        metricsService.tagAvailabilityData(m1, tags, start.getMillis(), start.plusMinutes(2).getMillis()).toBlocking();

        verifyTTLDataAccess.setAvailabilityTTL(days(14).toStandardSeconds().getSeconds());
        Availability m2 = new Availability("t2", new MetricId("m2"));
        m2.addData(new AvailabilityData(start.plusMinutes(5).getMillis(), UP));
        addDataInThePast(m2, days(5).toStandardDuration());

        verifyTTLDataAccess.availabilityTagTLLLessThanEqualTo(days(14).minus(5).toStandardSeconds().getSeconds());
        metricsService.tagAvailabilityData(m2, tags, start.plusMinutes(5).getMillis()).toBlocking();

        metricsService.createTenant(new Tenant().setId("t3").setRetention(AVAILABILITY, 24))
                      .toBlocking()
                      .lastOrDefault(null);
        verifyTTLDataAccess.setAvailabilityTTL(hours(24).toStandardSeconds().getSeconds());
        Availability m3 = new Availability("t3", new MetricId("m3"));
        m3.addData(new AvailabilityData(start.getMillis(), UP));
        getUninterruptibly(metricsService.addAvailabilityData(singletonList(m3)));
    }

    private void addDataInThePast(Gauge metric, final Duration duration) throws Exception {
        DataAccess originalDataAccess = metricsService.getDataAccess();
        try {
            metricsService.setDataAccess(new DelegatingDataAccess(dataAccess) {

                @Override
                public Observable<ResultSet> insertData(Observable<GaugeAndTTL> observable) {
                    List<ResultSetFuture> futures = new ArrayList<>();
                    for (GaugeAndTTL pair : observable.toBlocking().toIterable()) {
                        futures.add(insertData(pair.gauge, pair.ttl));
                    }
                    return Observable.from(futures).flatMap(Observable::from);
                }

                public ResultSetFuture insertData(Gauge m, int ttl) {
                    int actualTTL = ttl - duration.toStandardSeconds().getSeconds();
                    long writeTime = now().minus(duration).getMillis() * 1000;
                    BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
                    for (GaugeData d : m.getData()) {
                        batchStatement.add(insertGaugeDataWithTimestamp.bind(m.getTenantId(), GAUGE.getCode(),
                                m.getId().getName(), m.getId().getInterval().toString(), DPART, d.getTimeUUID(),
                                d.getValue(), actualTTL, writeTime));
                    }
                    return session.executeAsync(batchStatement);
                }
            });
            metricsService.addGaugeData(Observable.just(metric));
        } finally {
            metricsService.setDataAccess(originalDataAccess);
        }
    }

    private void addDataInThePast(Availability metric, final Duration duration) throws Exception {
        DataAccess originalDataAccess = metricsService.getDataAccess();
        try {
            metricsService.setDataAccess(new DelegatingDataAccess(dataAccess) {
                @Override
                public ResultSetFuture insertData(Availability m, int ttl) {
                    int actualTTL = ttl - duration.toStandardSeconds().getSeconds();
                    long writeTime = now().minus(duration).getMillis() * 1000;
                    BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
                    for (AvailabilityData a : m.getData()) {
                        batchStatement.add(insertAvailabilityDateWithTimestamp.bind(m.getTenantId(),
                            AVAILABILITY.getCode(), m.getId().getName(), m.getId().getInterval().toString(), DPART,
                            a.getTimeUUID(), a.getBytes(), actualTTL, writeTime));
                    }
                    return session.executeAsync(batchStatement);
                }
            });
            metricsService.addAvailabilityData(singletonList(metric));
        } finally {
            metricsService.setDataAccess(originalDataAccess);
        }
    }

    @Test
    public void fetchGaugeDataThatHasTags() throws Exception {
        DateTime end = now();
        DateTime start = end.minusMinutes(10);

        metricsService.createTenant(new Tenant().setId("tenant1")).toBlocking().lastOrDefault(null);

        Gauge metric = new Gauge("tenant1", new MetricId("m1"));
        metric.addData(start.getMillis(), 100.0);
        metric.addData(start.plusMinutes(1).getMillis(), 101.1);
        metric.addData(start.plusMinutes(2).getMillis(), 102.2);
        metric.addData(start.plusMinutes(3).getMillis(), 103.3);
        metric.addData(start.plusMinutes(4).getMillis(), 104.4);
        metric.addData(start.plusMinutes(5).getMillis(), 105.5);
        metric.addData(start.plusMinutes(6).getMillis(), 106.6);

        metricsService.addGaugeData(Observable.just(metric)).toBlocking().lastOrDefault(null);

        Map<String, String> tags1 = ImmutableMap.of("t1", "1", "t2", "");
        metricsService.tagGaugeData(metric, tags1, start.plusMinutes(2).getMillis()).toBlocking().lastOrDefault(null);

        Map<String, String> tags2 = ImmutableMap.of("t3", "3", "t4", "");
        metricsService.tagGaugeData(metric, tags2, start.plusMinutes(3).getMillis(), start.plusMinutes(5).getMillis()
        ).toBlocking().lastOrDefault(null);

        Observable<GaugeData> observable = metricsService.findGaugeData("tenant1", new MetricId("m1"),
                start.getMillis(), end.getMillis());
        List<GaugeData> actual = ImmutableList.copyOf(observable.toBlocking().toIterable());
        List<GaugeData> expected = asList(
            new GaugeData(start.plusMinutes(6).getMillis(), 106.6),
            new GaugeData(start.plusMinutes(5).getMillis(), 105.5),
            new GaugeData(start.plusMinutes(4).getMillis(), 104.4),
            new GaugeData(start.plusMinutes(3).getMillis(), 103.3),
            new GaugeData(start.plusMinutes(2).getMillis(), 102.2),
            new GaugeData(start.plusMinutes(1).getMillis(), 101.1),
            new GaugeData(start.getMillis(), 100.0)
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

        metricsService.createTenant(new Tenant().setId(tenantId)).toBlocking().lastOrDefault(null);

        Gauge m1 = new Gauge(tenantId, new MetricId("m1"));
        m1.addData(new GaugeData(start.plusSeconds(30).getMillis(), 11.2));
        m1.addData(new GaugeData(start.getMillis(), 11.1));

        Gauge m2 = new Gauge(tenantId, new MetricId("m2"));
        m2.addData(new GaugeData(start.plusSeconds(30).getMillis(), 12.2));
        m2.addData(new GaugeData(start.getMillis(), 12.1));

        Gauge m3 = new Gauge(tenantId, new MetricId("m3"));

        Gauge m4 = new Gauge(tenantId, new MetricId("m4"), emptyMap(), 24);
        metricsService.createMetric(m4).toBlocking().lastOrDefault(null);
        m4.addData(new GaugeData(start.plusSeconds(30).getMillis(), 55.5));
        m4.addData(new GaugeData(end.getMillis(), 66.6));

        metricsService.addGaugeData(Observable.just(m1, m2, m3, m4)).toBlocking().lastOrDefault(null);

        Observable<GaugeData> observable1 = metricsService.findGaugeData(tenantId, m1.getId(),
                start.getMillis(), end.getMillis());
        assertEquals(toList(observable1), m1.getData(), "The gauge data for " + m1.getId() + " does not match");

        Observable<GaugeData> observable2 = metricsService.findGaugeData(tenantId, m2.getId(), start.getMillis(),
                end.getMillis());
        assertEquals(toList(observable2), m2.getData(), "The gauge data for " + m2.getId() + " does not match");

        Observable<GaugeData> observable3 = metricsService.findGaugeData(tenantId, m3.getId(), start.getMillis(),
                end.getMillis());
        assertTrue(toList(observable3).isEmpty(), "Did not expect to get back results for " + m3.getId());

        Observable<GaugeData> observable4 = metricsService.findGaugeData(tenantId, m4.getId(), start.getMillis(),
                end.getMillis());
        Gauge expected = new Gauge(tenantId, new MetricId("m4"));
        expected.setDataRetention(24);
        expected.addData(start.plusSeconds(30).getMillis(), 55.5);
        assertEquals(toList(observable4), expected.getData(), "The gauge data for " + m4.getId() + " does not match");

        assertMetricIndexMatches(tenantId, GAUGE, asList(m1, m2, m3, m4));
    }

    @Test
    public void addAvailabilityForMultipleMetrics() throws Exception {
        DateTime start = now().minusMinutes(10);
        DateTime end = start.plusMinutes(8);
        String tenantId = "test-tenant";

        metricsService.createTenant(new Tenant().setId(tenantId)).toBlocking().lastOrDefault(null);

        Availability m1 = new Availability(tenantId, new MetricId("m1"));
        m1.addData(new AvailabilityData(start.plusSeconds(10).getMillis(), "up"));
        m1.addData(new AvailabilityData(start.plusSeconds(20).getMillis(), "down"));

        Availability m2 = new Availability(tenantId, new MetricId("m2"));
        m2.addData(new AvailabilityData(start.plusSeconds(15).getMillis(), "down"));
        m2.addData(new AvailabilityData(start.plusSeconds(30).getMillis(), "up"));

        Availability m3 = new Availability(tenantId, new MetricId("m3"));

        ListenableFuture<Void> insertFuture = metricsService.addAvailabilityData(asList(m1, m2, m3));
        getUninterruptibly(insertFuture);

        ListenableFuture<List<AvailabilityData>> queryFuture = metricsService.findAvailabilityData(tenantId, m1.getId(),
                start.getMillis(), end.getMillis());
        List<AvailabilityData> actual = getUninterruptibly(queryFuture);
        assertEquals(actual, m1.getData(), "The availability data does not match expected values");

        queryFuture = metricsService.findAvailabilityData(tenantId, m2.getId(), start.getMillis(), end.getMillis());
        actual = getUninterruptibly(queryFuture);
        assertEquals(actual, m2.getData(), "The availability data does not match expected values");

        queryFuture = metricsService.findAvailabilityData(tenantId, m3.getId(), start.getMillis(), end.getMillis());
        actual = getUninterruptibly(queryFuture);
        assertEquals(actual.size(), 0, "Did not expect to get back results since there is no data for " + m3);

        Availability m4 = new Availability(tenantId, new MetricId("m4"), emptyMap(), 24);
        metricsService.createMetric(m4).toBlocking().lastOrDefault(null);
        m4.addData(new AvailabilityData(start.plusMinutes(2).getMillis(), UP));
        m4.addData(new AvailabilityData(end.plusMinutes(2).getMillis(), UP));

        insertFuture = metricsService.addAvailabilityData(singletonList(m4));
        getUninterruptibly(insertFuture);

        queryFuture = metricsService.findAvailabilityData(tenantId, m4.getId(), start.getMillis(), end.getMillis());
        actual = getUninterruptibly(queryFuture);
        Availability expected = new Availability(tenantId, m4.getId(), emptyMap(), 24);
        expected.addData(new AvailabilityData(start.plusMinutes(2).getMillis(), UP));
        assertEquals(actual, expected.getData(), "The availability data does not match expected values");

        assertMetricIndexMatches(tenantId, AVAILABILITY, asList(m1, m2, m3, m4));
    }

    @Test
    public void fetchAvailabilityDataThatHasTags() throws Exception {
        DateTime end = now();
        DateTime start = end.minusMinutes(10);

        metricsService.createTenant(new Tenant().setId("tenant1")).toBlocking().lastOrDefault(null);

        Availability metric = new Availability("tenant1", new MetricId("A1"));
        metric.addData(new AvailabilityData(start.getMillis(), UP));
        metric.addData(new AvailabilityData(start.plusMinutes(1).getMillis(), DOWN));
        metric.addData(new AvailabilityData(start.plusMinutes(2).getMillis(), DOWN));
        metric.addData(new AvailabilityData(start.plusMinutes(3).getMillis(), UP));
        metric.addData(new AvailabilityData(start.plusMinutes(4).getMillis(), DOWN));
        metric.addData(new AvailabilityData(start.plusMinutes(5).getMillis(), UP));
        metric.addData(new AvailabilityData(start.plusMinutes(6).getMillis(), UP));

        ListenableFuture<Void> insertFuture = metricsService.addAvailabilityData(singletonList(metric));
        getUninterruptibly(insertFuture);

        Map<String, String> tags1 = ImmutableMap.of("t1", "1", "t2", "");
        metricsService.tagAvailabilityData(metric, tags1, start.plusMinutes(2).getMillis()).toBlocking()
                .lastOrDefault(null);

        Map<String, String> tags2 = ImmutableMap.of("t3", "3", "t4", "");
        metricsService.tagAvailabilityData(metric, tags2, start.plusMinutes(3).getMillis(),
            start.plusMinutes(5).getMillis()).toBlocking().lastOrDefault(null);

        ListenableFuture<List<AvailabilityData>> queryFuture = metricsService.findAvailabilityData("tenant1",
                metric.getId(), start.getMillis(), end.getMillis());
        List<AvailabilityData> actual = getUninterruptibly(queryFuture);
        List<AvailabilityData> expected = asList(
            new AvailabilityData(start.getMillis(), UP),
            new AvailabilityData(start.plusMinutes(1).getMillis(), DOWN),
            new AvailabilityData(start.plusMinutes(2).getMillis(), DOWN),
            new AvailabilityData(start.plusMinutes(3).getMillis(), UP),
            new AvailabilityData(start.plusMinutes(4).getMillis(), DOWN),
            new AvailabilityData(start.plusMinutes(5).getMillis(), UP),
            new AvailabilityData(start.plusMinutes(6).getMillis(), UP)
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
        MetricId metricId = new MetricId("A1");

        metricsService.createTenant(new Tenant().setId(tenantId)).toBlocking().lastOrDefault(null);

        Availability metric = new Availability("tenant1", metricId);
        metric.addData(new AvailabilityData(start.getMillis(), UP));
        metric.addData(new AvailabilityData(start.plusMinutes(1).getMillis(), DOWN));
        metric.addData(new AvailabilityData(start.plusMinutes(2).getMillis(), DOWN));
        metric.addData(new AvailabilityData(start.plusMinutes(3).getMillis(), UP));
        metric.addData(new AvailabilityData(start.plusMinutes(4).getMillis(), DOWN));
        metric.addData(new AvailabilityData(start.plusMinutes(5).getMillis(), UP));
        metric.addData(new AvailabilityData(start.plusMinutes(6).getMillis(), UP));
        metric.addData(new AvailabilityData(start.plusMinutes(7).getMillis(), UNKNOWN));
        metric.addData(new AvailabilityData(start.plusMinutes(8).getMillis(), UNKNOWN));
        metric.addData(new AvailabilityData(start.plusMinutes(9).getMillis(), DOWN));
        metric.addData(new AvailabilityData(start.plusMinutes(10).getMillis(), UP));

        ListenableFuture<Void> insertFuture = metricsService.addAvailabilityData(singletonList(metric));
        getUninterruptibly(insertFuture);

        List<AvailabilityData> actual = getUninterruptibly(metricsService.findAvailabilityData(tenantId, metricId,
                start.getMillis(), end.getMillis(), true));

        List<AvailabilityData> expected = asList(
            metric.getData().get(0),
            metric.getData().get(1),
            metric.getData().get(3),
            metric.getData().get(4),
            metric.getData().get(5),
            metric.getData().get(7),
            metric.getData().get(9),
            metric.getData().get(10)
        );

        assertEquals(actual, expected, "The availability data does not match the expected values");
    }

    @Test
    public void tagGaugeDataByDateRangeAndQueryByMultipleTags() throws Exception {
        String tenant = "tag-test";
        DateTime start = now().minusMinutes(20);

        metricsService.createTenant(new Tenant().setId(tenant)).toBlocking().lastOrDefault(null);

        GaugeData d1 = new GaugeData(start.getMillis(), 101.1);
        GaugeData d2 = new GaugeData(start.plusMinutes(2).getMillis(), 101.2);
        GaugeData d3 = new GaugeData(start.plusMinutes(6).getMillis(), 102.2);
        GaugeData d4 = new GaugeData(start.plusMinutes(8).getMillis(), 102.3);
        GaugeData d5 = new GaugeData(start.plusMinutes(4).getMillis(), 102.1);
        GaugeData d6 = new GaugeData(start.plusMinutes(4).getMillis(), 101.4);
        GaugeData d7 = new GaugeData(start.plusMinutes(10).getMillis(), 102.4);
        GaugeData d8 = new GaugeData(start.plusMinutes(6).getMillis(), 103.1);
        GaugeData d9 = new GaugeData(start.plusMinutes(7).getMillis(), 103.1);

        Gauge m1 = new Gauge(tenant, new MetricId("m1"));
        m1.addData(d1);
        m1.addData(d2);
        m1.addData(d6);

        Gauge m2 = new Gauge(tenant, new MetricId("m2"));
        m2.addData(d3);
        m2.addData(d4);
        m2.addData(d5);
        m2.addData(d7);

        Gauge m3 = new Gauge(tenant, new MetricId("m3"));
        m3.addData(d8);
        m3.addData(d9);

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

        Map<MetricId, Set<GaugeData>> actual = metricsService
            .findGaugeDataByTags(tenant, ImmutableMap.of("t1", "1", "t2", "2")).toBlocking().toIterable().iterator()
            .next();
        ImmutableMap<MetricId, ImmutableSet<GaugeData>> expected = ImmutableMap.of(
            new MetricId("m1"), ImmutableSet.of(d1, d2, d6),
            new MetricId("m2"), ImmutableSet.of(d5, d3)
        );

        assertEquals(actual, expected, "The tagged data does not match");
    }

    @Test
    public void tagAvailabilityByDateRangeAndQueryByMultipleTags() throws Exception {
        String tenant = "tag-test";
        DateTime start = now().minusMinutes(20);

        metricsService.createTenant(new Tenant().setId(tenant)).toBlocking().lastOrDefault(null);

        Availability m1 = new Availability(tenant, new MetricId("m1"));
        Availability m2 = new Availability(tenant, new MetricId("m2"));
        Availability m3 = new Availability(tenant, new MetricId("m3"));

        AvailabilityData a1 = new AvailabilityData(start.getMillis(), UP);
        AvailabilityData a2 = new AvailabilityData(start.plusMinutes(2).getMillis(), UP);
        AvailabilityData a3 = new AvailabilityData(start.plusMinutes(6).getMillis(), DOWN);
        AvailabilityData a4 = new AvailabilityData(start.plusMinutes(8).getMillis(), DOWN);
        AvailabilityData a5 = new AvailabilityData(start.plusMinutes(4).getMillis(), UP);
        AvailabilityData a6 = new AvailabilityData(start.plusMinutes(4).getMillis(), DOWN);
        AvailabilityData a7 = new AvailabilityData(start.plusMinutes(10).getMillis(), UP);
        AvailabilityData a8 = new AvailabilityData(start.plusMinutes(6).getMillis(), DOWN);
        AvailabilityData a9 = new AvailabilityData(start.plusMinutes(7).getMillis(), UP);

        m1.addData(a1);
        m1.addData(a2);
        m1.addData(a6);

        m2.addData(a3);
        m2.addData(a4);
        m2.addData(a5);
        m2.addData(a7);

        m3.addData(a8);
        m3.addData(a9);

        ListenableFuture<Void> insertFuture = metricsService.addAvailabilityData(asList(m1, m2, m3));
        getUninterruptibly(insertFuture);

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

        Map<MetricId, Set<AvailabilityData>> actual = metricsService
            .findAvailabilityByTags(tenant, ImmutableMap.of("t1", "1", "t2", "2")).toBlocking().toIterable().iterator
                        ().next();
        ImmutableMap<MetricId, ImmutableSet<AvailabilityData>> expected = ImmutableMap.of(
                new MetricId("m1"), ImmutableSet.of(a1, a2, a6),
                new MetricId("m2"), ImmutableSet.of(a5, a3)
        );

        assertEquals(actual, expected, "The tagged data does not match");
    }

    @Test
    public void tagIndividualGaugeDataPoints() throws Exception {
        String tenant = "tag-test";
        DateTime start = now().minusMinutes(20);

        metricsService.createTenant(new Tenant().setId(tenant)).toBlocking().lastOrDefault(null);

        GaugeData d1 = new GaugeData(start.getMillis(), 101.1);
        GaugeData d2 = new GaugeData(start.plusMinutes(2).getMillis(), 101.2);
        GaugeData d3 = new GaugeData(start.plusMinutes(6).getMillis(), 102.2);
        GaugeData d4 = new GaugeData(start.plusMinutes(8).getMillis(), 102.3);
        GaugeData d5 = new GaugeData(start.plusMinutes(4).getMillis(), 102.1);
        GaugeData d6 = new GaugeData(start.plusMinutes(4).getMillis(), 101.4);
        GaugeData d7 = new GaugeData(start.plusMinutes(10).getMillis(), 102.4);
        GaugeData d8 = new GaugeData(start.plusMinutes(6).getMillis(), 103.1);
        GaugeData d9 = new GaugeData(start.plusMinutes(7).getMillis(), 103.1);

        Gauge m1 = new Gauge(tenant, new MetricId("m1"));
        m1.addData(d1);
        m1.addData(d2);
        m1.addData(d6);

        Gauge m2 = new Gauge(tenant, new MetricId("m2"));
        m2.addData(d3);
        m2.addData(d4);
        m2.addData(d5);
        m2.addData(d7);

        Gauge m3 = new Gauge(tenant, new MetricId("m3"));
        m3.addData(d8);
        m3.addData(d9);


        metricsService.addGaugeData(Observable.just(m1, m2, m3)).toBlocking().lastOrDefault(null);

        Map<String, String> tags1 = ImmutableMap.of("t1", "");
        ResultSet tagFuture = metricsService.tagGaugeData(m1, tags1, d1.getTimestamp())
                .toBlocking().lastOrDefault(null);
//        assertTrue(tagFuture.wasApplied(), "Tagging " + d1 + " returned unexpected results");

        Map<String, String> tags2 = ImmutableMap.of("t1", "", "t2", "", "t3", "");
        tagFuture = metricsService.tagGaugeData(m1, tags2, d2.getTimestamp()).toBlocking().lastOrDefault(null);
//        assertTrue(tagFuture.wasApplied(), "Tagging " + d2 + " returned unexpected results");

//        tagFuture = metricsService.tagGaugeData(m1, tags1, start.minusMinutes(10).getMillis());
//        assertEquals(getUninterruptibly(tagFuture), Collections.emptyList(),
//                "No data should be returned since there is no data for this time");

        Map<String, String> tags3 = ImmutableMap.of("t1", "", "t2", "");
        tagFuture = metricsService.tagGaugeData(m2, tags3, d3.getTimestamp()).toBlocking().lastOrDefault(null);
//        assertTrue(tagFuture.wasApplied(), "Tagging " + d3 + " returned unexpected results");

        Map<String, String> tags4 = ImmutableMap.of("t3", "", "t4", "");
        tagFuture = metricsService.tagGaugeData(m2, tags4, d4.getTimestamp()).toBlocking().lastOrDefault(null);
//        assertTrue(tagFuture.wasApplied(), "Tagging " + d4 + " returned unexpected results");

        Map<MetricId, Set<GaugeData>> actual = metricsService.findGaugeDataByTags(tenant,
                ImmutableMap.of("t2", "", "t3", "")).toBlocking().lastOrDefault(null);

        ImmutableMap<MetricId, ImmutableSet<GaugeData>> expected = ImmutableMap.of(
                new MetricId("m1"), ImmutableSet.of(d2),
                new MetricId("m2"), ImmutableSet.of(d3, d4)
        );

        assertEquals(actual, expected, "The tagged data does not match");
    }

    @Test
    public void tagIndividualAvailabilityDataPoints() throws Exception {
        String tenant = "tag-test";
        DateTime start = now().minusMinutes(20);

        metricsService.createTenant(new Tenant().setId(tenant)).toBlocking().lastOrDefault(null);

        Availability m1 = new Availability(tenant, new MetricId("m1"));
        Availability m2 = new Availability(tenant, new MetricId("m2"));
        Availability m3 = new Availability(tenant, new MetricId("m3"));

        AvailabilityData a1 = new AvailabilityData(start.getMillis(), UP);
        AvailabilityData a2 = new AvailabilityData(start.plusMinutes(2).getMillis(), UP);
        AvailabilityData a3 = new AvailabilityData(start.plusMinutes(6).getMillis(), DOWN);
        AvailabilityData a4 = new AvailabilityData(start.plusMinutes(8).getMillis(), DOWN);
        AvailabilityData a5 = new AvailabilityData(start.plusMinutes(4).getMillis(), UP);
        AvailabilityData a6 = new AvailabilityData(start.plusMinutes(4).getMillis(), DOWN);
        AvailabilityData a7 = new AvailabilityData(start.plusMinutes(10).getMillis(), UP);
        AvailabilityData a8 = new AvailabilityData(start.plusMinutes(6).getMillis(), DOWN);
        AvailabilityData a9 = new AvailabilityData(start.plusMinutes(7).getMillis(), UP);

        m1.addData(a1);
        m1.addData(a2);
        m1.addData(a6);

        m2.addData(a3);
        m2.addData(a4);
        m2.addData(a5);
        m2.addData(a7);

        m3.addData(a8);
        m3.addData(a9);

        ListenableFuture<Void> insertFuture = metricsService.addAvailabilityData(asList(m1, m2, m3));
        getUninterruptibly(insertFuture);

        Map<String, String> tags1 = ImmutableMap.of("t1", "");
        ResultSet tagFuture = metricsService.tagAvailabilityData(m1, tags1, a1.getTimestamp()).toBlocking().last();
//        assertTrue(tagFuture.wasApplied(), "Tagging " + a1 + " returned unexpected results");

        Map<String, String> tags2 = ImmutableMap.of("t1", "", "t2", "", "t3", "");
        tagFuture = metricsService.tagAvailabilityData(m1, tags2, a2.getTimestamp()).toBlocking().last();
//        assertTrue(tagFuture.wasApplied(), "Tagging " + a2 + " returned unexpected results");

//        tagFuture = metricsService.tagAvailabilityData(m1, tags1, start.minusMinutes(10).getMillis());
//        assertEquals(getUninterruptibly(tagFuture), Collections.emptyList(),
//            "No data should be returned since there is no data for this time");

        Map<String, String> tags3 = ImmutableMap.of("t2", "", "t3", "");
        tagFuture = metricsService.tagAvailabilityData(m2, tags3, a3.getTimestamp()).toBlocking().last();
//        assertTrue(tagFuture.wasApplied(), "Tagging " + a3 + " returned unexpected results");

        Map<String, String> tags4 = ImmutableMap.of("t3", "", "t4", "");
        metricsService.tagAvailabilityData(m2, tags4, a4.getTimestamp()).toBlocking().last();
//        assertTrue(tagFuture.wasApplied(), "Tagging " + a4 + " returned unexpected results");

        Map<MetricId, Set<AvailabilityData>> actual = metricsService.findAvailabilityByTags(
                tenant, tags3).toBlocking().last();
        ImmutableMap<MetricId, ImmutableSet<AvailabilityData>> expected = ImmutableMap.of(
            new MetricId("m1"), ImmutableSet.of(a2),
            new MetricId("m2"), ImmutableSet.of(a3, a4)
        );

        assertEquals(actual, expected, "The tagged data does not match");
    }

    @Test
    public void getPeriodsAboveThreshold() throws Exception {
        String tenantId = "test-tenant";
        DateTime start = now().minusMinutes(20);
        double threshold = 20.0;

        Gauge m1 = new Gauge(tenantId, new MetricId("m1"));
        m1.addData(start.getMillis(), 14.0);
        m1.addData(start.plusMinutes(1).getMillis(), 18.0);
        m1.addData(start.plusMinutes(2).getMillis(), 21.0);
        m1.addData(start.plusMinutes(3).getMillis(), 23.0);
        m1.addData(start.plusMinutes(4).getMillis(), 20.0);
        m1.addData(start.plusMinutes(5).getMillis(), 19.0);
        m1.addData(start.plusMinutes(6).getMillis(), 22.0);
        m1.addData(start.plusMinutes(7).getMillis(), 15.0);
        m1.addData(start.plusMinutes(8).getMillis(), 31.0);
        m1.addData(start.plusMinutes(9).getMillis(), 30.0);
        m1.addData(start.plusMinutes(10).getMillis(), 31.0);

        metricsService.addGaugeData(Observable.just(m1)).toBlocking().lastOrDefault(null);

        List<long[]> actual = getUninterruptibly(metricsService.getPeriods(tenantId, m1.getId(),
            value -> value > threshold, start.getMillis(), now().getMillis()));
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

    private void assertMetricEquals(Metric actual, Metric expected) {
        assertEquals(actual, expected, "The metric doe not match the expected value");
        assertEquals(actual.getData(), expected.getData(), "The data does not match the expected values");
    }

    private void assertMetricIndexMatches(String tenantId, MetricType type, List<? extends Metric> expected)
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
        ResultSet resultSet = dataAccess.findMetricsByTag(tenantId, tag).toBlocking().first();
        List<MetricsTagsIndexEntry> actual = new ArrayList<>();

        for (Row row : resultSet) {
            actual.add(new MetricsTagsIndexEntry(row.getString(0), MetricType.fromCode(row.getInt(1)),
                    new MetricId(row.getString(2), Interval.parse(row.getString(3)))));
        }

        assertEquals(actual, expected, "The metrics tags index entries do not match");
    }

    private void assertDataRetentionsIndexMatches(String tenantId, MetricType type, Set<Retention> expected)
        throws Exception {
        ResultSetFuture queryFuture = dataAccess.findDataRetentions(tenantId, type);
        ListenableFuture<Set<Retention>> retentionsFuture = Futures.transform(queryFuture, new DataRetentionsMapper());
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
        public Observable<ResultSet> insertData(Observable<GaugeAndTTL> observable) {
            observable.forEach(pair -> assertEquals(pair.ttl, gaugeTTL,
                    "The gauge TTL does not match the expected value when inserting data"));
            return super.insertData(observable);
        }

        @Override
        public ResultSetFuture insertData(Availability metric, int ttl) {
            assertEquals(ttl, availabilityTTL, "The availability data TTL does not match the expected value when " +
                "inserting data");
            return super.insertData(metric, ttl);
        }

        @Override
        public Observable<ResultSet> insertGaugeTag(String tag, String tagValue, Gauge metric,
                                                    Observable<GaugeData> data) {

            List<GaugeData> first = data.toList().toBlocking().first();

            for (GaugeData d : first) {
                assertTrue(d.getTTL() <= gaugeTagTTL, "Expected the TTL to be <= " + gaugeTagTTL +
                    " but it was " + d.getTTL());
            }
            return super.insertGaugeTag(tag, tagValue, metric, data);
        }

        @Override
        public Observable<ResultSet> insertAvailabilityTag(String tag, String tagValue, Availability metric,
                                                           Observable<AvailabilityData> data) {
            List<AvailabilityData> first = data.toList().toBlocking().first();
            for (AvailabilityData a : first) {
                assertTrue(a.getTTL() <= availabilityTagTTL, "Expected the TTL to be <= " + availabilityTagTTL +
                    " but it was " + a.getTTL());
            }
            return super.insertAvailabilityTag(tag, tagValue, metric, data);
        }
    }

}
