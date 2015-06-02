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
import static org.hawkular.metrics.core.impl.DataAccessImpl.DPART;
import static org.hawkular.metrics.core.api.MetricType.AVAILABILITY;
import static org.hawkular.metrics.core.api.MetricType.GAUGE;
import static org.hawkular.metrics.core.impl.TimeUUIDUtils.getTimeUUID;
import static org.hawkular.metrics.core.impl.MetricsServiceImpl.DEFAULT_TTL;
import static org.joda.time.DateTime.now;
import static org.joda.time.Days.days;
import static org.joda.time.Hours.hours;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import org.hawkular.metrics.core.api.AvailabilityDataPoint;
import org.hawkular.metrics.core.api.GaugeDataPoint;
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
        Metric<GaugeDataPoint> m1 = new MetricImpl<>("t1", GAUGE, new MetricId("m1"),
                ImmutableMap.of("a1", "1", "a2", "2"), 24);
        metricsService.createMetric(m1).toBlocking().lastOrDefault(null);

        Metric actual = metricsService.findMetric(m1.getTenantId(), m1.getType(), m1.getId()).toBlocking().last();
        assertEquals(actual, m1, "The metric does not match the expected value");

        Metric<AvailabilityDataPoint> m2 = new MetricImpl<>("t1", AVAILABILITY, new MetricId("m2"),
                ImmutableMap.of("a3", "3", "a4", "3"), DEFAULT_TTL);
        metricsService.createMetric(m2).toBlocking().lastOrDefault(null);

        Metric actualAvail = metricsService.findMetric(m2.getTenantId(), m2.getType(), m2.getId()).toBlocking().last();
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

        Metric<GaugeDataPoint> m3 = new MetricImpl<>("t1", GAUGE, new MetricId("m3"), emptyMap(), 24);
        metricsService.createMetric(m3).toBlocking().lastOrDefault(null);

        Metric<GaugeDataPoint> m4 = new MetricImpl<>("t1", GAUGE, new MetricId("m4"),
                ImmutableMap.of("a1", "A", "a2", ""), null);
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
        MetricImpl<GaugeDataPoint> metric = new MetricImpl<>("t1", GAUGE, new MetricId("m1"),
                ImmutableMap.of("a1", "1", "a2", "2"), DEFAULT_TTL);
        metricsService.createMetric(metric).toBlocking().lastOrDefault(null);

        Map<String, String> additions = ImmutableMap.of("a2", "two", "a3", "3");
        metricsService.addTags(metric, additions).toBlocking().lastOrDefault
                (null);

        Map<String, String> deletions = ImmutableMap.of("a1", "1");
        metricsService.deleteTags(metric, deletions).toBlocking()
                .lastOrDefault(null);

        Metric updatedMetric = metricsService.findMetric(metric.getTenantId(), GAUGE, metric.getId()).toBlocking()
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

        metricsService.createTenant(new Tenant().setId(tenantId)).toBlocking().lastOrDefault(null);

        MetricImpl<GaugeDataPoint> m1 = new MetricImpl<GaugeDataPoint>(tenantId, GAUGE, new MetricId("m1"))
                .addDataPoint(new GaugeDataPoint(start.getMillis(), 1.1))
                .addDataPoint(new GaugeDataPoint(start.plusMinutes(2).getMillis(), 2.2))
                .addDataPoint(new GaugeDataPoint(start.plusMinutes(4).getMillis(), 3.3))
                .addDataPoint(new GaugeDataPoint(end.getMillis(), 4.4));

        Observable<Void> insertObservable = metricsService.addGaugeData(Observable.just(m1));
        insertObservable.toBlocking().lastOrDefault(null);

        Observable<GaugeDataPoint> observable = metricsService.findGaugeData("t1", new MetricId("m1"),
                start.getMillis(), end.getMillis());
        List<GaugeDataPoint> actual = toList(observable);
        List<GaugeDataPoint> expected = asList(
                new GaugeDataPoint(start.plusMinutes(4).getMillis(), 3.3),
                new GaugeDataPoint(start.plusMinutes(2).getMillis(), 2.2),
                new GaugeDataPoint(start.getMillis(), 1.1)
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

        Metric<GaugeDataPoint> m1 = new MetricImpl<GaugeDataPoint>("t1", GAUGE, new MetricId("m1"))
                .addDataPoint(new GaugeDataPoint(start.getMillis(), 1.01))
                .addDataPoint(new GaugeDataPoint(start.plusMinutes(1).getMillis(), 1.02))
                .addDataPoint(new GaugeDataPoint(start.plusMinutes(2).getMillis(), 1.03));

        addGaugeDataInThePast(m1, days(2).toStandardDuration());

        Map<String, String> tags = ImmutableMap.of("tag1", "");

        verifyTTLDataAccess.gaugeTagTTLLessThanEqualTo(DEFAULT_TTL - days(2).toStandardSeconds().getSeconds());
        metricsService.tagGaugeData(m1, tags, start.getMillis(),
            start.plusMinutes(2).getMillis()).toBlocking();

        verifyTTLDataAccess.setGaugeTTL(days(14).toStandardSeconds().getSeconds());
        MetricImpl<GaugeDataPoint> m2 = new MetricImpl<>("t2", GAUGE, new MetricId("m2"));
        m2.addDataPoint(new GaugeDataPoint(start.plusMinutes(5).getMillis(), 2.02));
        addGaugeDataInThePast(m2, days(3).toStandardDuration());

        verifyTTLDataAccess.gaugeTagTTLLessThanEqualTo(days(14).minus(3).toStandardSeconds().getSeconds());
        metricsService.tagGaugeData(m2, tags, start.plusMinutes(5).getMillis()).toBlocking().last();

        metricsService.createTenant(new Tenant().setId("t3").setRetention(GAUGE, 24))
                      .toBlocking()
                      .lastOrDefault(null);
        verifyTTLDataAccess.setGaugeTTL(hours(24).toStandardSeconds().getSeconds());
        MetricImpl<GaugeDataPoint> m3 = new MetricImpl<>("t3", GAUGE, new MetricId("m3"));
        m3.addDataPoint(new GaugeDataPoint(start.getMillis(), 3.03));
        metricsService.addGaugeData(Observable.just(m3)).toBlocking().lastOrDefault(null);

        MetricImpl<GaugeDataPoint> m4 = new MetricImpl<>("t2", GAUGE, new MetricId("m4"), emptyMap(), 28);
        metricsService.createMetric(m4).toBlocking().lastOrDefault(null);

        verifyTTLDataAccess.setGaugeTTL(28);
        m4.addDataPoint(new GaugeDataPoint(start.plusMinutes(3).getMillis(), 4.1));
        m4.addDataPoint(new GaugeDataPoint(start.plusMinutes(4).getMillis(), 4.2));
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

        MetricImpl<AvailabilityDataPoint> m1 = new MetricImpl<AvailabilityDataPoint>("t1", AVAILABILITY,
                new MetricId("m1"))
            .addDataPoint(new AvailabilityDataPoint(start.getMillis(), UP))
            .addDataPoint(new AvailabilityDataPoint(start.plusMinutes(1).getMillis(), DOWN))
            .addDataPoint(new AvailabilityDataPoint(start.plusMinutes(2).getMillis(), DOWN));

        addAvailabilityDataInThePast(m1, days(2).toStandardDuration());

        Map<String, String> tags = ImmutableMap.of("tag1", "");

        verifyTTLDataAccess.availabilityTagTLLLessThanEqualTo(DEFAULT_TTL - days(2).toStandardSeconds().getSeconds());
        metricsService.tagAvailabilityData(m1, tags, start.getMillis(), start.plusMinutes(2).getMillis()).toBlocking();

        verifyTTLDataAccess.setAvailabilityTTL(days(14).toStandardSeconds().getSeconds());
        MetricImpl<AvailabilityDataPoint> m2 = new MetricImpl<AvailabilityDataPoint>("t2", AVAILABILITY,
                new MetricId("m2")).addDataPoint(new AvailabilityDataPoint(start.plusMinutes(5).getMillis(), UP));

        addAvailabilityDataInThePast(m2, days(5).toStandardDuration());

        verifyTTLDataAccess.availabilityTagTLLLessThanEqualTo(days(14).minus(5).toStandardSeconds().getSeconds());
        metricsService.tagAvailabilityData(m2, tags, start.plusMinutes(5).getMillis()).toBlocking();

        metricsService.createTenant(new Tenant().setId("t3").setRetention(AVAILABILITY, 24))
                      .toBlocking()
                      .lastOrDefault(null);
        verifyTTLDataAccess.setAvailabilityTTL(hours(24).toStandardSeconds().getSeconds());
        MetricImpl<AvailabilityDataPoint> m3 = new MetricImpl<AvailabilityDataPoint>("t3", AVAILABILITY,
                new MetricId("m3")).addDataPoint(new AvailabilityDataPoint(start.getMillis(), UP));

        metricsService.addAvailabilityData(singletonList(m3)).toBlocking();
    }

    private void addGaugeDataInThePast(Metric<GaugeDataPoint> metric, final Duration duration) throws Exception {
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

                public ResultSetFuture insertData(Metric<GaugeDataPoint> m, int ttl) {
                    int actualTTL = ttl - duration.toStandardSeconds().getSeconds();
                    long writeTime = now().minus(duration).getMillis() * 1000;
                    BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
                    for (GaugeDataPoint d : m.getDataPoints()) {
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

    private void addAvailabilityDataInThePast(Metric<AvailabilityDataPoint> metric, final Duration duration)
            throws Exception {
        DataAccess originalDataAccess = metricsService.getDataAccess();
        try {
            metricsService.setDataAccess(new DelegatingDataAccess(dataAccess) {
                @Override
                public Observable<ResultSet> insertAvailabilityData(Metric<AvailabilityDataPoint> m, int ttl) {
                    int actualTTL = ttl - duration.toStandardSeconds().getSeconds();
                    long writeTime = now().minus(duration).getMillis() * 1000;
                    BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
                    for (AvailabilityDataPoint a : m.getDataPoints()) {
                        batchStatement.add(insertAvailabilityDateWithTimestamp.bind(m.getTenantId(),
                            AVAILABILITY.getCode(), m.getId().getName(), m.getId().getInterval().toString(), DPART,
                            getTimeUUID(a.getTimestamp()), getBytes(a), actualTTL, writeTime));
                    }
                    return rxSession.execute(batchStatement);
                }
            });
            metricsService.addAvailabilityData(singletonList(metric));
        } finally {
            metricsService.setDataAccess(originalDataAccess);
        }
    }

    private ByteBuffer getBytes(AvailabilityDataPoint dataPoint) {
        return ByteBuffer.wrap(new byte[]{dataPoint.getValue().getCode()});
    }

    @Test
    public void fetchGaugeDataThatHasTags() throws Exception {
        DateTime end = now();
        DateTime start = end.minusMinutes(10);

        metricsService.createTenant(new Tenant().setId("tenant1")).toBlocking().lastOrDefault(null);

        MetricImpl<GaugeDataPoint> metric = new MetricImpl<GaugeDataPoint>("tenant1", GAUGE, new MetricId("m1"))
            .addDataPoint(new GaugeDataPoint(start.getMillis(), 100.0))
            .addDataPoint(new GaugeDataPoint(start.plusMinutes(1).getMillis(), 101.1))
            .addDataPoint(new GaugeDataPoint(start.plusMinutes(2).getMillis(), 102.2))
            .addDataPoint(new GaugeDataPoint(start.plusMinutes(3).getMillis(), 103.3))
            .addDataPoint(new GaugeDataPoint(start.plusMinutes(4).getMillis(), 104.4))
            .addDataPoint(new GaugeDataPoint(start.plusMinutes(5).getMillis(), 105.5))
            .addDataPoint(new GaugeDataPoint(start.plusMinutes(6).getMillis(), 106.6));

        metricsService.addGaugeData(Observable.just(metric)).toBlocking().lastOrDefault(null);

        Map<String, String> tags1 = ImmutableMap.of("t1", "1", "t2", "");
        metricsService.tagGaugeData(metric, tags1, start.plusMinutes(2).getMillis()).toBlocking().lastOrDefault(null);

        Map<String, String> tags2 = ImmutableMap.of("t3", "3", "t4", "");
        metricsService.tagGaugeData(metric, tags2, start.plusMinutes(3).getMillis(), start.plusMinutes(5).getMillis()
        ).toBlocking().lastOrDefault(null);

        Observable<GaugeDataPoint> observable = metricsService.findGaugeData("tenant1", new MetricId("m1"),
                start.getMillis(), end.getMillis());
        List<GaugeDataPoint> actual = ImmutableList.copyOf(observable.toBlocking().toIterable());
        List<GaugeDataPoint> expected = asList(
            new GaugeDataPoint(start.plusMinutes(6).getMillis(), 106.6),
            new GaugeDataPoint(start.plusMinutes(5).getMillis(), 105.5),
            new GaugeDataPoint(start.plusMinutes(4).getMillis(), 104.4),
            new GaugeDataPoint(start.plusMinutes(3).getMillis(), 103.3),
            new GaugeDataPoint(start.plusMinutes(2).getMillis(), 102.2),
            new GaugeDataPoint(start.plusMinutes(1).getMillis(), 101.1),
            new GaugeDataPoint(start.getMillis(), 100.0)
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

        MetricImpl<GaugeDataPoint> m1 = new MetricImpl<GaugeDataPoint>(tenantId, GAUGE, new MetricId("m1"))
                .addDataPoint(new GaugeDataPoint(start.plusSeconds(30).getMillis(), 11.2))
                .addDataPoint(new GaugeDataPoint(start.getMillis(), 11.1));

        MetricImpl<GaugeDataPoint> m2 = new MetricImpl<GaugeDataPoint>(tenantId, GAUGE, new MetricId("m2"))
                .addDataPoint(new GaugeDataPoint(start.plusSeconds(30).getMillis(), 12.2))
                .addDataPoint(new GaugeDataPoint(start.getMillis(), 12.1));


        MetricImpl<GaugeDataPoint> m3 = new MetricImpl<>(tenantId, GAUGE, new MetricId("m3"));

        MetricImpl<GaugeDataPoint> m4 = new MetricImpl<>(tenantId, GAUGE, new MetricId("m4"), emptyMap(), 24);
        metricsService.createMetric(m4).toBlocking().lastOrDefault(null);
        m4.addDataPoint(new GaugeDataPoint(start.plusSeconds(30).getMillis(), 55.5));
        m4.addDataPoint(new GaugeDataPoint(end.getMillis(), 66.6));

        metricsService.addGaugeData(Observable.just(m1, m2, m3, m4)).toBlocking().lastOrDefault(null);

        Observable<GaugeDataPoint> observable1 = metricsService.findGaugeData(tenantId, m1.getId(),
                start.getMillis(), end.getMillis());
        assertEquals(toList(observable1), m1.getDataPoints(), "The gauge data for " + m1.getId() + " does not match");

        Observable<GaugeDataPoint> observable2 = metricsService.findGaugeData(tenantId, m2.getId(), start.getMillis(),
                end.getMillis());
        assertEquals(toList(observable2), m2.getDataPoints(), "The gauge data for " + m2.getId() + " does not match");

        Observable<GaugeDataPoint> observable3 = metricsService.findGaugeData(tenantId, m3.getId(), start.getMillis(),
                end.getMillis());
        assertTrue(toList(observable3).isEmpty(), "Did not expect to get back results for " + m3.getId());

        Observable<GaugeDataPoint> observable4 = metricsService.findGaugeData(tenantId, m4.getId(), start.getMillis(),
                end.getMillis());
        MetricImpl<GaugeDataPoint> expected = new MetricImpl<>(tenantId, GAUGE, new MetricId("m4"),
                Collections.emptyMap(), 24);
        expected.addDataPoint(new GaugeDataPoint(start.plusSeconds(30).getMillis(), 55.5));
        assertEquals(toList(observable4), expected.getDataPoints(),
                "The gauge data for " + m4.getId() + " does not match");

        assertMetricIndexMatches(tenantId, GAUGE, asList(m1, m2, m3, m4));
    }

    @Test
    public void addAvailabilityForMultipleMetrics() throws Exception {
        DateTime start = now().minusMinutes(10);
        DateTime end = start.plusMinutes(8);
        String tenantId = "test-tenant";

        metricsService.createTenant(new Tenant().setId(tenantId)).toBlocking().lastOrDefault(null);

        MetricImpl<AvailabilityDataPoint> m1 = new MetricImpl<AvailabilityDataPoint>(tenantId, AVAILABILITY,
                new MetricId("m1"))
            .addDataPoint(new AvailabilityDataPoint(start.plusSeconds(10).getMillis(), "up"))
            .addDataPoint(new AvailabilityDataPoint(start.plusSeconds(20).getMillis(), "down"));

        MetricImpl<AvailabilityDataPoint> m2 = new MetricImpl<AvailabilityDataPoint>(tenantId, AVAILABILITY,
                new MetricId("m2"))
            .addDataPoint(new AvailabilityDataPoint(start.plusSeconds(15).getMillis(), "down"))
            .addDataPoint(new AvailabilityDataPoint(start.plusSeconds(30).getMillis(), "up"));

        MetricImpl<AvailabilityDataPoint> m3 = new MetricImpl<>(tenantId, AVAILABILITY, new MetricId("m3"));

        metricsService.addAvailabilityData(asList(m1, m2, m3)).toBlocking().lastOrDefault(null);

        List<AvailabilityDataPoint> actual = metricsService.findAvailabilityData(tenantId, m1.getId(), start.getMillis
                (), end.getMillis()).toList().toBlocking().last();
        assertEquals(actual, m1.getDataPoints(), "The availability data does not match expected values");

        actual = metricsService.findAvailabilityData(tenantId, m2.getId(), start.getMillis(), end.getMillis()).toList()
                .toBlocking().last();
        assertEquals(actual, m2.getDataPoints(), "The availability data does not match expected values");

        actual = metricsService.findAvailabilityData(tenantId, m3.getId(), start.getMillis(), end.getMillis()).toList()
            .toBlocking().last();
        assertEquals(actual.size(), 0, "Did not expect to get back results since there is no data for " + m3);

        MetricImpl<AvailabilityDataPoint> m4 = new MetricImpl<>(tenantId, AVAILABILITY, new MetricId("m4"), emptyMap(),
                24);
        metricsService.createMetric(m4).toBlocking().lastOrDefault(null);
        m4.addDataPoint(new AvailabilityDataPoint(start.plusMinutes(2).getMillis(), UP));
        m4.addDataPoint(new AvailabilityDataPoint(end.plusMinutes(2).getMillis(), UP));

        metricsService.addAvailabilityData(singletonList(m4)).toBlocking().lastOrDefault(null);

        actual = metricsService.findAvailabilityData(tenantId, m4.getId(), start.getMillis(), end.getMillis()).toList()
            .toBlocking().last();
        MetricImpl<AvailabilityDataPoint> expected = new MetricImpl<>(tenantId, AVAILABILITY, m4.getId(), emptyMap(),
                24);
        expected.addDataPoint(new AvailabilityDataPoint(start.plusMinutes(2).getMillis(), UP));
        assertEquals(actual, expected.getDataPoints(), "The availability data does not match expected values");

        assertMetricIndexMatches(tenantId, AVAILABILITY, asList(m1, m2, m3, m4));
    }

    @Test
    public void fetchAvailabilityDataThatHasTags() throws Exception {
        DateTime end = now();
        DateTime start = end.minusMinutes(10);

        metricsService.createTenant(new Tenant().setId("tenant1")).toBlocking().lastOrDefault(null);

        MetricImpl<AvailabilityDataPoint> metric = new MetricImpl<AvailabilityDataPoint>("tenant1", AVAILABILITY,
                new MetricId("A1"))
            .addDataPoint(new AvailabilityDataPoint(start.getMillis(), UP))
            .addDataPoint(new AvailabilityDataPoint(start.plusMinutes(1).getMillis(), DOWN))
            .addDataPoint(new AvailabilityDataPoint(start.plusMinutes(2).getMillis(), DOWN))
            .addDataPoint(new AvailabilityDataPoint(start.plusMinutes(3).getMillis(), UP))
            .addDataPoint(new AvailabilityDataPoint(start.plusMinutes(4).getMillis(), DOWN))
            .addDataPoint(new AvailabilityDataPoint(start.plusMinutes(5).getMillis(), UP))
            .addDataPoint(new AvailabilityDataPoint(start.plusMinutes(6).getMillis(), UP));

        metricsService.addAvailabilityData(singletonList(metric)).toBlocking().lastOrDefault(null);

        Map<String, String> tags1 = ImmutableMap.of("t1", "1", "t2", "");
        metricsService.tagAvailabilityData(metric, tags1, start.plusMinutes(2).getMillis()).toBlocking()
                .lastOrDefault(null);

        Map<String, String> tags2 = ImmutableMap.of("t3", "3", "t4", "");
        metricsService.tagAvailabilityData(metric, tags2, start.plusMinutes(3).getMillis(),
            start.plusMinutes(5).getMillis()).toBlocking().lastOrDefault(null);

        List<AvailabilityDataPoint> actual = metricsService.findAvailabilityData("tenant1",
                metric.getId(), start.getMillis(), end.getMillis()).toList().toBlocking().last();
        List<AvailabilityDataPoint> expected = asList(
            new AvailabilityDataPoint(start.getMillis(), UP),
            new AvailabilityDataPoint(start.plusMinutes(1).getMillis(), DOWN),
            new AvailabilityDataPoint(start.plusMinutes(2).getMillis(), DOWN),
            new AvailabilityDataPoint(start.plusMinutes(3).getMillis(), UP),
            new AvailabilityDataPoint(start.plusMinutes(4).getMillis(), DOWN),
            new AvailabilityDataPoint(start.plusMinutes(5).getMillis(), UP),
            new AvailabilityDataPoint(start.plusMinutes(6).getMillis(), UP)
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

        MetricImpl<AvailabilityDataPoint> metric = new MetricImpl<AvailabilityDataPoint>("tenant1", AVAILABILITY,
                metricId)
            .addDataPoint(new AvailabilityDataPoint(start.getMillis(), UP))
            .addDataPoint(new AvailabilityDataPoint(start.plusMinutes(1).getMillis(), DOWN))
            .addDataPoint(new AvailabilityDataPoint(start.plusMinutes(2).getMillis(), DOWN))
            .addDataPoint(new AvailabilityDataPoint(start.plusMinutes(3).getMillis(), UP))
            .addDataPoint(new AvailabilityDataPoint(start.plusMinutes(4).getMillis(), DOWN))
            .addDataPoint(new AvailabilityDataPoint(start.plusMinutes(5).getMillis(), UP))
            .addDataPoint(new AvailabilityDataPoint(start.plusMinutes(6).getMillis(), UP))
            .addDataPoint(new AvailabilityDataPoint(start.plusMinutes(7).getMillis(), UNKNOWN))
            .addDataPoint(new AvailabilityDataPoint(start.plusMinutes(8).getMillis(), UNKNOWN))
            .addDataPoint(new AvailabilityDataPoint(start.plusMinutes(9).getMillis(), DOWN))
            .addDataPoint(new AvailabilityDataPoint(start.plusMinutes(10).getMillis(), UP));

        metricsService.addAvailabilityData(singletonList(metric)).toBlocking().lastOrDefault(null);

        List<AvailabilityDataPoint> actual = metricsService.findAvailabilityData(tenantId, metricId,
                start.getMillis(), end.getMillis(), true).toList().toBlocking().lastOrDefault(null);

        List<AvailabilityDataPoint> expected = asList(
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

        metricsService.createTenant(new Tenant().setId(tenant)).toBlocking().lastOrDefault(null);

        GaugeDataPoint d1 = new GaugeDataPoint(start.getMillis(), 101.1);
        GaugeDataPoint d2 = new GaugeDataPoint(start.plusMinutes(2).getMillis(), 101.2);
        GaugeDataPoint d3 = new GaugeDataPoint(start.plusMinutes(6).getMillis(), 102.2);
        GaugeDataPoint d4 = new GaugeDataPoint(start.plusMinutes(8).getMillis(), 102.3);
        GaugeDataPoint d5 = new GaugeDataPoint(start.plusMinutes(4).getMillis(), 102.1);
        GaugeDataPoint d6 = new GaugeDataPoint(start.plusMinutes(4).getMillis(), 101.4);
        GaugeDataPoint d7 = new GaugeDataPoint(start.plusMinutes(10).getMillis(), 102.4);
        GaugeDataPoint d8 = new GaugeDataPoint(start.plusMinutes(6).getMillis(), 103.1);
        GaugeDataPoint d9 = new GaugeDataPoint(start.plusMinutes(7).getMillis(), 103.1);

        MetricImpl<GaugeDataPoint> m1 = new MetricImpl<>(tenant, GAUGE, new MetricId("m1"));
        m1.addDataPoint(d1);
        m1.addDataPoint(d2);
        m1.addDataPoint(d6);

        MetricImpl<GaugeDataPoint> m2 = new MetricImpl<>(tenant, GAUGE, new MetricId("m2"));
        m2.addDataPoint(d3);
        m2.addDataPoint(d4);
        m2.addDataPoint(d5);
        m2.addDataPoint(d7);

        MetricImpl<GaugeDataPoint> m3 = new MetricImpl<>(tenant, GAUGE, new MetricId("m3"));
        m3.addDataPoint(d8);
        m3.addDataPoint(d9);

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

        Map<MetricId, Set<GaugeDataPoint>> actual = metricsService
            .findGaugeDataByTags(tenant, ImmutableMap.of("t1", "1", "t2", "2")).toBlocking().toIterable().iterator()
            .next();
        ImmutableMap<MetricId, ImmutableSet<GaugeDataPoint>> expected = ImmutableMap.of(
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

        MetricImpl<AvailabilityDataPoint> m1 = new MetricImpl<>(tenant, AVAILABILITY, new MetricId("m1"));
        MetricImpl<AvailabilityDataPoint> m2 = new MetricImpl<>(tenant, AVAILABILITY, new MetricId("m2"));
        MetricImpl<AvailabilityDataPoint> m3 = new MetricImpl<>(tenant, AVAILABILITY, new MetricId("m3"));

        AvailabilityDataPoint a1 = new AvailabilityDataPoint(start.getMillis(), UP);
        AvailabilityDataPoint a2 = new AvailabilityDataPoint(start.plusMinutes(2).getMillis(), UP);
        AvailabilityDataPoint a3 = new AvailabilityDataPoint(start.plusMinutes(6).getMillis(), DOWN);
        AvailabilityDataPoint a4 = new AvailabilityDataPoint(start.plusMinutes(8).getMillis(), DOWN);
        AvailabilityDataPoint a5 = new AvailabilityDataPoint(start.plusMinutes(4).getMillis(), UP);
        AvailabilityDataPoint a6 = new AvailabilityDataPoint(start.plusMinutes(4).getMillis(), DOWN);
        AvailabilityDataPoint a7 = new AvailabilityDataPoint(start.plusMinutes(10).getMillis(), UP);
        AvailabilityDataPoint a8 = new AvailabilityDataPoint(start.plusMinutes(6).getMillis(), DOWN);
        AvailabilityDataPoint a9 = new AvailabilityDataPoint(start.plusMinutes(7).getMillis(), UP);

        m1.addDataPoint(a1).addDataPoint(a2).addDataPoint(a6);
        m2.addDataPoint(a3).addDataPoint(a4).addDataPoint(a5).addDataPoint(a7);
        m3.addDataPoint(a8).addDataPoint(a9);

        metricsService.addAvailabilityData(asList(m1, m2, m3)).toBlocking().lastOrDefault(null);

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

        Map<MetricId, Set<AvailabilityDataPoint>> actual = metricsService
            .findAvailabilityByTags(tenant, ImmutableMap.of("t1", "1", "t2", "2")).toBlocking().toIterable().iterator
                        ().next();
        ImmutableMap<MetricId, ImmutableSet<AvailabilityDataPoint>> expected = ImmutableMap.of(
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

        GaugeDataPoint d1 = new GaugeDataPoint(start.getMillis(), 101.1);
        GaugeDataPoint d2 = new GaugeDataPoint(start.plusMinutes(2).getMillis(), 101.2);
        GaugeDataPoint d3 = new GaugeDataPoint(start.plusMinutes(6).getMillis(), 102.2);
        GaugeDataPoint d4 = new GaugeDataPoint(start.plusMinutes(8).getMillis(), 102.3);
        GaugeDataPoint d5 = new GaugeDataPoint(start.plusMinutes(4).getMillis(), 102.1);
        GaugeDataPoint d6 = new GaugeDataPoint(start.plusMinutes(4).getMillis(), 101.4);
        GaugeDataPoint d7 = new GaugeDataPoint(start.plusMinutes(10).getMillis(), 102.4);
        GaugeDataPoint d8 = new GaugeDataPoint(start.plusMinutes(6).getMillis(), 103.1);
        GaugeDataPoint d9 = new GaugeDataPoint(start.plusMinutes(7).getMillis(), 103.1);

        MetricImpl<GaugeDataPoint> m1 = new MetricImpl<GaugeDataPoint>(tenant, GAUGE, new MetricId("m1"))
            .addDataPoint(d1)
            .addDataPoint(d2)
            .addDataPoint(d6);

        MetricImpl<GaugeDataPoint> m2 = new MetricImpl<GaugeDataPoint>(tenant, GAUGE, new MetricId("m2"))
            .addDataPoint(d3)
            .addDataPoint(d4)
            .addDataPoint(d5)
            .addDataPoint(d7);

        MetricImpl<GaugeDataPoint> m3 = new MetricImpl<GaugeDataPoint>(tenant, GAUGE, new MetricId("m3"))
            .addDataPoint(d8)
            .addDataPoint(d9);


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

        Map<MetricId, Set<GaugeDataPoint>> actual = metricsService.findGaugeDataByTags(tenant,
                ImmutableMap.of("t2", "", "t3", "")).toBlocking().lastOrDefault(null);

        ImmutableMap<MetricId, ImmutableSet<GaugeDataPoint>> expected = ImmutableMap.of(
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

        MetricImpl<AvailabilityDataPoint> m1 = new MetricImpl<>(tenant, AVAILABILITY, new MetricId("m1"));
        MetricImpl<AvailabilityDataPoint> m2 = new MetricImpl<>(tenant, AVAILABILITY, new MetricId("m2"));
        MetricImpl<AvailabilityDataPoint> m3 = new MetricImpl<>(tenant, AVAILABILITY, new MetricId("m3"));

        AvailabilityDataPoint a1 = new AvailabilityDataPoint(start.getMillis(), UP);
        AvailabilityDataPoint a2 = new AvailabilityDataPoint(start.plusMinutes(2).getMillis(), UP);
        AvailabilityDataPoint a3 = new AvailabilityDataPoint(start.plusMinutes(6).getMillis(), DOWN);
        AvailabilityDataPoint a4 = new AvailabilityDataPoint(start.plusMinutes(8).getMillis(), DOWN);
        AvailabilityDataPoint a5 = new AvailabilityDataPoint(start.plusMinutes(4).getMillis(), UP);
        AvailabilityDataPoint a6 = new AvailabilityDataPoint(start.plusMinutes(4).getMillis(), DOWN);
        AvailabilityDataPoint a7 = new AvailabilityDataPoint(start.plusMinutes(10).getMillis(), UP);
        AvailabilityDataPoint a8 = new AvailabilityDataPoint(start.plusMinutes(6).getMillis(), DOWN);
        AvailabilityDataPoint a9 = new AvailabilityDataPoint(start.plusMinutes(7).getMillis(), UP);

        m1.addDataPoint(a1);
        m1.addDataPoint(a2);
        m1.addDataPoint(a6);

        m2.addDataPoint(a3);
        m2.addDataPoint(a4);
        m2.addDataPoint(a5);
        m2.addDataPoint(a7);

        m3.addDataPoint(a8);
        m3.addDataPoint(a9);

        metricsService.addAvailabilityData(asList(m1, m2, m3)).toBlocking().lastOrDefault(null);

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

        Map<MetricId, Set<AvailabilityDataPoint>> actual = metricsService.findAvailabilityByTags(
                tenant, tags3).toBlocking().last();
        ImmutableMap<MetricId, ImmutableSet<AvailabilityDataPoint>> expected = ImmutableMap.of(
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

        Metric<GaugeDataPoint> m1 = new MetricImpl<GaugeDataPoint>(tenantId, GAUGE, new MetricId("m1"))
                .addDataPoint(new GaugeDataPoint(start.getMillis(), 14.0))
                .addDataPoint(new GaugeDataPoint(start.plusMinutes(1).getMillis(), 18.0))
                .addDataPoint(new GaugeDataPoint(start.plusMinutes(2).getMillis(), 21.0))
                .addDataPoint(new GaugeDataPoint(start.plusMinutes(3).getMillis(), 23.0))
                .addDataPoint(new GaugeDataPoint(start.plusMinutes(3).getMillis(), 23.0))
                .addDataPoint(new GaugeDataPoint(start.plusMinutes(4).getMillis(), 20.0))
                .addDataPoint(new GaugeDataPoint(start.plusMinutes(5).getMillis(), 19.0))
                .addDataPoint(new GaugeDataPoint(start.plusMinutes(6).getMillis(), 22.0))
                .addDataPoint(new GaugeDataPoint(start.plusMinutes(7).getMillis(), 15.0))
                .addDataPoint(new GaugeDataPoint(start.plusMinutes(8).getMillis(), 31.0))
                .addDataPoint(new GaugeDataPoint(start.plusMinutes(9).getMillis(), 30.0))
                .addDataPoint(new GaugeDataPoint(start.plusMinutes(10).getMillis(), 31.0));

        metricsService.addGaugeData(Observable.just(m1)).toBlocking().lastOrDefault(null);

        List<long[]> actual = metricsService.getPeriods(tenantId, m1.getId(),
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
        public Observable<ResultSet> insertAvailabilityData(Metric<AvailabilityDataPoint> metric, int ttl) {
            assertEquals(ttl, availabilityTTL, "The availability data TTL does not match the expected value when " +
                "inserting data");
            return super.insertAvailabilityData(metric, ttl);
        }

        @Override
        public Observable<ResultSet> insertGaugeTag(String tag, String tagValue, Metric<GaugeDataPoint> metric,
                Observable<TTLDataPoint<GaugeDataPoint>> data) {

            List<TTLDataPoint<GaugeDataPoint>> first = data.toList().toBlocking().first();

            for (TTLDataPoint<GaugeDataPoint> d : first) {
                assertTrue(d.getTTL() <= gaugeTagTTL, "Expected the TTL to be <= " + gaugeTagTTL +
                    " but it was " + d.getTTL());
            }
            return super.insertGaugeTag(tag, tagValue, metric, data);
        }

        @Override
        public Observable<ResultSet> insertAvailabilityTag(String tag, String tagValue,
                Metric<AvailabilityDataPoint> metric, Observable<TTLDataPoint<AvailabilityDataPoint>> data) {
            List<TTLDataPoint<AvailabilityDataPoint>> first = data.toList().toBlocking().first();
            for (TTLDataPoint<AvailabilityDataPoint> a : first) {
                assertTrue(a.getTTL() <= availabilityTagTTL, "Expected the TTL to be <= " + availabilityTagTTL +
                    " but it was " + a.getTTL());
            }
            return super.insertAvailabilityTag(tag, tagValue, metric, data);
        }
    }

}
