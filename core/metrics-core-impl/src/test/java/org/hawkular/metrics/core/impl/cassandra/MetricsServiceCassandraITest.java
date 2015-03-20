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

import static org.hawkular.metrics.core.api.AvailabilityType.DOWN;
import static org.hawkular.metrics.core.api.AvailabilityType.UP;
import static org.hawkular.metrics.core.api.Metric.DPART;
import static org.hawkular.metrics.core.api.MetricType.AVAILABILITY;
import static org.hawkular.metrics.core.api.MetricType.NUMERIC;
import static org.hawkular.metrics.core.impl.cassandra.MetricsServiceCassandra.DEFAULT_TTL;
import static org.joda.time.DateTime.now;
import static org.joda.time.Days.days;
import static org.joda.time.Hours.hours;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.hawkular.metrics.core.api.Availability;
import org.hawkular.metrics.core.api.AvailabilityMetric;
import org.hawkular.metrics.core.api.Interval;
import org.hawkular.metrics.core.api.Metric;
import org.hawkular.metrics.core.api.MetricAlreadyExistsException;
import org.hawkular.metrics.core.api.MetricId;
import org.hawkular.metrics.core.api.MetricType;
import org.hawkular.metrics.core.api.NumericData;
import org.hawkular.metrics.core.api.NumericMetric;
import org.hawkular.metrics.core.api.Retention;
import org.hawkular.metrics.core.api.Tenant;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author John Sanda
 */
public class MetricsServiceCassandraITest extends MetricsITest {

    private MetricsServiceCassandra metricsService;

    private DataAccess dataAccess;

    private PreparedStatement insertNumericDataWithTimestamp;

    private PreparedStatement insertAvailabilityDateWithTimestamp;

    @BeforeClass
    public void initClass() {
        initSession();
        metricsService = new MetricsServiceCassandra();
        metricsService.startUp(session);
        dataAccess = metricsService.getDataAccess();

        insertNumericDataWithTimestamp = session.prepare(
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
        Tenant t1 = new Tenant().setId("t1").setRetention(NUMERIC, 24).setRetention(AVAILABILITY, 24);
        Tenant t2 = new Tenant().setId("t2").setRetention(NUMERIC, 72);
        Tenant t3 = new Tenant().setId("t3").setRetention(AVAILABILITY, 48);
        Tenant t4 = new Tenant().setId("t4");

        List<ListenableFuture<Void>> insertFutures = new ArrayList<>();
        insertFutures.add(metricsService.createTenant(t1));
        insertFutures.add(metricsService.createTenant(t2));
        insertFutures.add(metricsService.createTenant(t3));
        insertFutures.add(metricsService.createTenant(t4));
        ListenableFuture<List<Void>> insertsFuture = Futures.allAsList(insertFutures);
        getUninterruptibly(insertsFuture);

        Collection<Tenant> tenants = getUninterruptibly(metricsService.getTenants());
        Set<Tenant> actualTenants = ImmutableSet.copyOf(tenants);
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

        assertDataRetentionsIndexMatches(t1.getId(), NUMERIC, ImmutableSet.of(new Retention(
            new MetricId("[" + NUMERIC.getText() + "]"), hours(24).toStandardSeconds().getSeconds())));
        assertDataRetentionsIndexMatches(t1.getId(), AVAILABILITY, ImmutableSet.of(new Retention(
            new MetricId("[" + AVAILABILITY.getText() + "]"), hours(24).toStandardSeconds().getSeconds())));
    }

    @Test
    public void createAndFindMetrics() throws Exception {
        NumericMetric m1 = new NumericMetric("t1", new MetricId("m1"), ImmutableMap.of("a1", "1", "a2", "2"),
            24);
        ListenableFuture<Void> insertFuture = metricsService.createMetric(m1);
        getUninterruptibly(insertFuture);

        ListenableFuture<Metric<?>> queryFuture = metricsService.findMetric(m1.getTenantId(), m1.getType(), m1.getId());
        Metric actual = getUninterruptibly(queryFuture);
        assertEquals(actual, m1, "The metric does not match the expected value");

        AvailabilityMetric m2 = new AvailabilityMetric("t1", new MetricId("m2"), ImmutableMap.of("a3", "3", "a4", "3"));
        insertFuture = metricsService.createMetric(m2);
        getUninterruptibly(insertFuture);

        queryFuture = metricsService.findMetric(m2.getTenantId(), m2.getType(), m2.getId());
        actual = getUninterruptibly(queryFuture);
        assertEquals(actual, m2, "The metric does not match the expected value");

        insertFuture = metricsService.createMetric(m1);
        Throwable exception = null;
        try {
            getUninterruptibly(insertFuture);
        } catch (Exception e) {
            exception = e.getCause();
        }
        assertTrue(exception != null && exception instanceof MetricAlreadyExistsException,
            "Expected a " + MetricAlreadyExistsException.class.getSimpleName() + " to be thrown");

        NumericMetric m3 = new NumericMetric("t1", new MetricId("m3"));
        m3.setDataRetention(24);
        insertFuture = metricsService.createMetric(m3);
        getUninterruptibly(insertFuture);

        NumericMetric m4 = new NumericMetric("t1", new MetricId("m4"), ImmutableMap.of("a1", "A", "a2", ""));
        insertFuture = metricsService.createMetric(m4);
        getUninterruptibly(insertFuture);

        assertMetricIndexMatches("t1", NUMERIC, asList(m1, m3, m4));
        assertMetricIndexMatches("t1", AVAILABILITY, asList(m2));

        assertDataRetentionsIndexMatches("t1", NUMERIC, ImmutableSet.of(new Retention(m3.getId(), 24),
            new Retention(m1.getId(), 24)));

        assertMetricsTagsIndexMatches("t1", "a1", asList(
            new MetricsTagsIndexEntry("1", NUMERIC, m1.getId()),
            new MetricsTagsIndexEntry("A", NUMERIC, m4.getId())
        ));
    }

    @Test
    public void updateMetricTags() throws Exception {
        NumericMetric metric = new NumericMetric("t1", new MetricId("m1"), ImmutableMap.of("a1", "1", "a2", "2"));
        ListenableFuture<Void> insertFuture = metricsService.createMetric(metric);
        getUninterruptibly(insertFuture);

        Map<String, String> additions = ImmutableMap.of("a2", "two", "a3", "3");
        insertFuture = metricsService.addTags(metric, additions);
        getUninterruptibly(insertFuture);

        Map<String, String> deletions = ImmutableMap.of("a1", "1");
        ListenableFuture<Void> deleteFuture = metricsService.deleteTags(metric, deletions);
        getUninterruptibly(deleteFuture);

        ListenableFuture<Metric<?>> queryFuture = metricsService.findMetric(metric.getTenantId(), NUMERIC,
            metric.getId());
        Metric<?> updatedMetric = getUninterruptibly(queryFuture);

        assertEquals(updatedMetric.getTags(), ImmutableMap.of("a2", "two", "a3", "3"),
            "The updated meta data does not match the expected values");

        assertMetricIndexMatches(metric.getTenantId(), NUMERIC, asList(updatedMetric));
    }

    @Test
    public void addAndFetchNumericData() throws Exception {
        DateTime start = now().minusMinutes(30);
        DateTime end = start.plusMinutes(20);

        getUninterruptibly(metricsService.createTenant(new Tenant().setId("t1")));

        NumericMetric m1 = new NumericMetric("t1", new MetricId("m1"));
        m1.addData(new NumericData(start.getMillis(), 1.1));
        m1.addData(new NumericData(start.plusMinutes(2).getMillis(), 2.2));
        m1.addData(new NumericData(start.plusMinutes(4).getMillis(), 3.3));
        m1.addData(new NumericData(end.getMillis(), 4.4));

        ListenableFuture<Void> insertFuture = metricsService.addNumericData(asList(m1));
        getUninterruptibly(insertFuture);

        ListenableFuture<List<NumericData>> queryFuture = metricsService.findData(m1, start.getMillis(),
            end.getMillis());
        List<NumericData> actual = getUninterruptibly(queryFuture);
        List<NumericData> expected = asList(
            new NumericData(start.plusMinutes(4).getMillis(), 3.3),
            new NumericData(start.plusMinutes(2).getMillis(), 2.2),
            new NumericData(start.getMillis(), 1.1)
        );

        assertEquals(actual, expected, "The data does not match the expected values");
        assertMetricIndexMatches("t1", NUMERIC, asList(m1));
    }

    @Test
    public void verifyTTLsSetOnNumericData() throws Exception {
        DateTime start = now().minusMinutes(10);

        getUninterruptibly(metricsService.createTenant(new Tenant().setId("t1")));
        getUninterruptibly(metricsService.createTenant(new Tenant().setId("t2")
            .setRetention(NUMERIC, days(14).toStandardHours().getHours())));

        VerifyTTLDataAccess verifyTTLDataAccess = new VerifyTTLDataAccess(dataAccess);

        metricsService.unloadDataRetentions();
        metricsService.loadDataRetentions();
        metricsService.setDataAccess(verifyTTLDataAccess);

        NumericMetric m1 = new NumericMetric("t1", new MetricId("m1"));
        m1.addData(start.getMillis(), 1.01);
        m1.addData(start.plusMinutes(1).getMillis(), 1.02);
        m1.addData(start.plusMinutes(2).getMillis(), 1.03);

        addDataInThePast(m1, days(2).toStandardDuration());

        Map<String, String> tags = ImmutableMap.of("tag1", "");

        verifyTTLDataAccess.numericTagTTLLessThanEqualTo(DEFAULT_TTL - days(2).toStandardSeconds().getSeconds());
        getUninterruptibly(metricsService.tagNumericData(m1, tags, start.getMillis(),
            start.plusMinutes(2).getMillis()));

        verifyTTLDataAccess.setNumericTTL(days(14).toStandardSeconds().getSeconds());
        NumericMetric m2 = new NumericMetric("t2", new MetricId("m2"));
        m2.addData(start.plusMinutes(5).getMillis(), 2.02);
        addDataInThePast(m2, days(3).toStandardDuration());

        verifyTTLDataAccess.numericTagTTLLessThanEqualTo(days(14).minus(3).toStandardSeconds().getSeconds());
        getUninterruptibly(metricsService.tagNumericData(m2, tags, start.plusMinutes(5).getMillis()));

        getUninterruptibly(metricsService.createTenant(new Tenant().setId("t3")
            .setRetention(NUMERIC, 24)));
        verifyTTLDataAccess.setNumericTTL(hours(24).toStandardSeconds().getSeconds());
        NumericMetric m3 = new NumericMetric("t3", new MetricId("m3"));
        m3.addData(start.getMillis(), 3.03);
        getUninterruptibly(metricsService.addNumericData(asList(m3)));

        NumericMetric m4 = new NumericMetric("t2", new MetricId("m4"), Collections.EMPTY_MAP, 28);
        getUninterruptibly(metricsService.createMetric(m4));

        verifyTTLDataAccess.setNumericTTL(28);
        m4.addData(start.plusMinutes(3).getMillis(), 4.1);
        m4.addData(start.plusMinutes(4).getMillis(), 4.2);
        getUninterruptibly(metricsService.addNumericData(asList(m4)));
    }

    @Test
    public void verifyTTLsSetOnAvailabilityData() throws Exception {
        DateTime start = now().minusMinutes(10);

        getUninterruptibly(metricsService.createTenant(new Tenant().setId("t1")));
        getUninterruptibly(metricsService.createTenant(new Tenant().setId("t2")
            .setRetention(AVAILABILITY, days(14).toStandardHours().getHours())));

        VerifyTTLDataAccess verifyTTLDataAccess = new VerifyTTLDataAccess(dataAccess);

        metricsService.unloadDataRetentions();
        metricsService.loadDataRetentions();
        metricsService.setDataAccess(verifyTTLDataAccess);
        metricsService.setDataAccess(verifyTTLDataAccess);

        AvailabilityMetric m1 = new AvailabilityMetric("t1", new MetricId("m1"));
        m1.addData(new Availability(start.getMillis(), UP));
        m1.addData(new Availability(start.plusMinutes(1).getMillis(), DOWN));
        m1.addData(new Availability(start.plusMinutes(2).getMillis(), DOWN));
        addDataInThePast(m1, days(2).toStandardDuration());

        Map<String, String> tags = ImmutableMap.of("tag1", "");

        verifyTTLDataAccess.availabilityTagTLLLessThanEqualTo(DEFAULT_TTL - days(2).toStandardSeconds().getSeconds());
        getUninterruptibly(metricsService.tagAvailabilityData(m1, tags, start.getMillis(),
            start.plusMinutes(2).getMillis()));

        verifyTTLDataAccess.setAvailabilityTTL(days(14).toStandardSeconds().getSeconds());
        AvailabilityMetric m2 = new AvailabilityMetric("t2", new MetricId("m2"));
        m2.addData(new Availability(start.plusMinutes(5).getMillis(), UP));
        addDataInThePast(m2, days(5).toStandardDuration());

        verifyTTLDataAccess.availabilityTagTLLLessThanEqualTo(days(14).minus(5).toStandardSeconds().getSeconds());
        getUninterruptibly(metricsService.tagAvailabilityData(m2, tags, start.plusMinutes(5).getMillis()));

        getUninterruptibly(metricsService.createTenant(new Tenant().setId("t3")
            .setRetention(AVAILABILITY, 24)));
        verifyTTLDataAccess.setAvailabilityTTL(hours(24).toStandardSeconds().getSeconds());
        AvailabilityMetric m3 = new AvailabilityMetric("t3", new MetricId("m3"));
        m3.addData(new Availability(start.getMillis(), UP));
        getUninterruptibly(metricsService.addAvailabilityData(asList(m3)));
    }

    private void addDataInThePast(NumericMetric metric, final Duration duration) throws Exception {
        DataAccess originalDataAccess = metricsService.getDataAccess();
        try {
            metricsService.setDataAccess(new DelegatingDataAccess(dataAccess) {
                @Override
                public ResultSetFuture insertData(NumericMetric m, int ttl) {
                    int actualTTL = ttl - duration.toStandardSeconds().getSeconds();
                    long writeTime = now().minus(duration).getMillis() * 1000;
                    BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
                    for (NumericData d : m.getData()) {
                        batchStatement.add(insertNumericDataWithTimestamp.bind(m.getTenantId(), NUMERIC.getCode(),
                                m.getId().getName(), m.getId().getInterval().toString(), DPART, d.getTimeUUID(),
                                d.getValue(), actualTTL, writeTime));
                    }
                    return session.executeAsync(batchStatement);
                }
            });
            metricsService.addNumericData(asList(metric));
        } finally {
            metricsService.setDataAccess(originalDataAccess);
        }
    }

    private void addDataInThePast(AvailabilityMetric metric, final Duration duration) throws Exception {
        DataAccess originalDataAccess = metricsService.getDataAccess();
        try {
            metricsService.setDataAccess(new DelegatingDataAccess(dataAccess) {
                @Override
                public ResultSetFuture insertData(AvailabilityMetric m, int ttl) {
                    int actualTTL = ttl - duration.toStandardSeconds().getSeconds();
                    long writeTime = now().minus(duration).getMillis() * 1000;
                    BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
                    for (Availability a : m.getData()) {
                        batchStatement.add(insertAvailabilityDateWithTimestamp.bind(m.getTenantId(),
                            AVAILABILITY.getCode(), m.getId().getName(), m.getId().getInterval().toString(), DPART,
                            a.getTimeUUID(), a.getBytes(), actualTTL, writeTime));
                    }
                    return session.executeAsync(batchStatement);
                }
            });
            metricsService.addAvailabilityData(asList(metric));
        } finally {
            metricsService.setDataAccess(originalDataAccess);
        }
    }

    @Test
    public void fetchNumericDataThatHasTags() throws Exception {
        DateTime end = now();
        DateTime start = end.minusMinutes(10);

        getUninterruptibly(metricsService.createTenant(new Tenant().setId("tenant1")));

        NumericMetric metric = new NumericMetric("tenant1", new MetricId("m1"));
        metric.addData(start.getMillis(), 100.0);
        metric.addData(start.plusMinutes(1).getMillis(), 101.1);
        metric.addData(start.plusMinutes(2).getMillis(), 102.2);
        metric.addData(start.plusMinutes(3).getMillis(), 103.3);
        metric.addData(start.plusMinutes(4).getMillis(), 104.4);
        metric.addData(start.plusMinutes(5).getMillis(), 105.5);
        metric.addData(start.plusMinutes(6).getMillis(), 106.6);

        ListenableFuture<Void> insertFuture = metricsService.addNumericData(asList(metric));
        getUninterruptibly(insertFuture);

        Map<String, String> tags1 = ImmutableMap.of("t1", "1", "t2", "");
        ListenableFuture<List<NumericData>> tagFuture = metricsService.tagNumericData(metric, tags1,
            start.plusMinutes(2).getMillis());
        getUninterruptibly(tagFuture);

        Map<String, String> tags2 = ImmutableMap.of("t3", "3", "t4", "");
        tagFuture = metricsService.tagNumericData(metric, tags2, start.plusMinutes(3).getMillis(),
            start.plusMinutes(5).getMillis());
        getUninterruptibly(tagFuture);

        ListenableFuture<List<NumericData>> queryFuture = metricsService.findData(metric, start.getMillis(),
            end.getMillis());
        List<NumericData> actual = getUninterruptibly(queryFuture);
        List<NumericData> expected = asList(
            new NumericData(start.plusMinutes(6).getMillis(), 106.6),
            new NumericData(start.plusMinutes(5).getMillis(), 105.5),
            new NumericData(start.plusMinutes(4).getMillis(), 104.4),
            new NumericData(start.plusMinutes(3).getMillis(), 103.3),
            new NumericData(start.plusMinutes(2).getMillis(), 102.2),
            new NumericData(start.plusMinutes(1).getMillis(), 101.1),
            new NumericData(start.getMillis(), 100.0)
        );

        assertEquals(actual, expected, "The data does not match the expected values");
        assertEquals(actual.get(3).getTags(), tags2, "The tags do not match");
        assertEquals(actual.get(2).getTags(), tags2, "The tags do not match");
        assertEquals(actual.get(2).getTags(), tags2, "The tags do not match");
        assertEquals(actual.get(4).getTags(), tags1, "The tags do not match");
    }

    @Test
    public void addNumericDataForMultipleMetrics() throws Exception {
        DateTime start = now().minusMinutes(10);
        DateTime end = start.plusMinutes(8);
        String tenantId = "test-tenant";

        getUninterruptibly(metricsService.createTenant(new Tenant().setId(tenantId)));

        NumericMetric m1 = new NumericMetric(tenantId, new MetricId("m1"));
        m1.addData(new NumericData(start.plusSeconds(30).getMillis(), 11.2));
        m1.addData(new NumericData(start.getMillis(), 11.1));

        NumericMetric m2 = new NumericMetric(tenantId, new MetricId("m2"));
        m2.addData(new NumericData(start.plusSeconds(30).getMillis(), 12.2));
        m2.addData(new NumericData(start.getMillis(), 12.1));

        NumericMetric m3 = new NumericMetric(tenantId, new MetricId("m3"));

        NumericMetric m4 = new NumericMetric(tenantId, new MetricId("m4"), Collections.EMPTY_MAP, 24);
        getUninterruptibly(metricsService.createMetric(m4));
        m4.addData(new NumericData(start.plusSeconds(30).getMillis(), 55.5));
        m4.addData(new NumericData(end.getMillis(), 66.6));

        ListenableFuture<Void> insertFuture = metricsService.addNumericData(asList(m1, m2, m3, m4));
        getUninterruptibly(insertFuture);

        ListenableFuture<NumericMetric> queryFuture = metricsService.findNumericData(m1, start.getMillis(),
            end.getMillis());
        NumericMetric actual = getUninterruptibly(queryFuture);
        assertMetricEquals(actual, m1);

        queryFuture = metricsService.findNumericData(m2, start.getMillis(), end.getMillis());
        actual = getUninterruptibly(queryFuture);
        assertMetricEquals(actual, m2);

        queryFuture = metricsService.findNumericData(m3, start.getMillis(), end.getMillis());
        actual = getUninterruptibly(queryFuture);
        assertNull(actual, "Did not expect to get back results since there is no data for " + m3);

        queryFuture = metricsService.findNumericData(m4, start.getMillis(), end.getMillis());
        actual = getUninterruptibly(queryFuture);
        NumericMetric expected = new NumericMetric(tenantId, new MetricId("m4"));
        expected.setDataRetention(24);
        expected.addData(start.plusSeconds(30).getMillis(), 55.5);
        assertMetricEquals(actual, expected);

        assertMetricIndexMatches(tenantId, NUMERIC, asList(m1, m2, m3, m4));
    }

    @Test
    public void addAvailabilityForMultipleMetrics() throws Exception {
        DateTime start = now().minusMinutes(10);
        DateTime end = start.plusMinutes(8);
        String tenantId = "test-tenant";

        getUninterruptibly(metricsService.createTenant(new Tenant().setId(tenantId)));

        AvailabilityMetric m1 = new AvailabilityMetric(tenantId, new MetricId("m1"));
        m1.addData(new Availability(start.plusSeconds(20).getMillis(), "down"));
        m1.addData(new Availability(start.plusSeconds(10).getMillis(), "up"));

        AvailabilityMetric m2 = new AvailabilityMetric(tenantId, new MetricId("m2"));
        m2.addData(new Availability(start.plusSeconds(30).getMillis(), "up"));
        m2.addData(new Availability(start.plusSeconds(15).getMillis(), "down"));

        AvailabilityMetric m3 = new AvailabilityMetric(tenantId, new MetricId("m3"));

        ListenableFuture<Void> insertFuture = metricsService.addAvailabilityData(asList(m1, m2, m3));
        getUninterruptibly(insertFuture);

        ListenableFuture<AvailabilityMetric> queryFuture = metricsService.findAvailabilityData(m1, start.getMillis(),
            end.getMillis());
        AvailabilityMetric actual = getUninterruptibly(queryFuture);
        assertMetricEquals(actual, m1);

        queryFuture = metricsService.findAvailabilityData(m2, start.getMillis(), end.getMillis());
        actual = getUninterruptibly(queryFuture);
        assertMetricEquals(actual, m2);

        queryFuture = metricsService.findAvailabilityData(m3, start.getMillis(), end.getMillis());
        actual = getUninterruptibly(queryFuture);
        assertNull(actual, "Did not expect to get back results since there is no data for " + m3);

        AvailabilityMetric m4 = new AvailabilityMetric(tenantId, new MetricId("m4"), Collections.EMPTY_MAP, 24);
        getUninterruptibly(metricsService.createMetric(m4));
        m4.addData(new Availability(start.plusMinutes(2).getMillis(), UP));
        m4.addData(new Availability(end.plusMinutes(2).getMillis(), UP));

        insertFuture = metricsService.addAvailabilityData(asList(m4));
        getUninterruptibly(insertFuture);

        queryFuture = metricsService.findAvailabilityData(m4, start.getMillis(), end.getMillis());
        actual = getUninterruptibly(queryFuture);
        AvailabilityMetric expected = new AvailabilityMetric(tenantId, m4.getId(), Collections.EMPTY_MAP, 24);
        expected.addData(new Availability(start.plusMinutes(2).getMillis(), UP));
        assertMetricEquals(actual, expected);

        assertMetricIndexMatches(tenantId, AVAILABILITY, asList(m1, m2, m3, m4));
    }

    @Test
    public void fetchAvailabilityDataThatHasTags() throws Exception {
        DateTime end = now();
        DateTime start = end.minusMinutes(10);

        getUninterruptibly(metricsService.createTenant(new Tenant().setId("tenant1")));

        AvailabilityMetric metric = new AvailabilityMetric("tenant1", new MetricId("A1"));
        metric.addData(new Availability(start.getMillis(), UP));
        metric.addData(new Availability(start.plusMinutes(1).getMillis(), DOWN));
        metric.addData(new Availability(start.plusMinutes(2).getMillis(), DOWN));
        metric.addData(new Availability(start.plusMinutes(3).getMillis(), UP));
        metric.addData(new Availability(start.plusMinutes(4).getMillis(), DOWN));
        metric.addData(new Availability(start.plusMinutes(5).getMillis(), UP));
        metric.addData(new Availability(start.plusMinutes(6).getMillis(), UP));

        ListenableFuture<Void> insertFuture = metricsService.addAvailabilityData(asList(metric));
        getUninterruptibly(insertFuture);

        Map<String, String> tags1 = ImmutableMap.of("t1", "1", "t2", "");
        ListenableFuture<List<Availability>> tagFuture = metricsService.tagAvailabilityData(metric, tags1,
            start.plusMinutes(2).getMillis());
        getUninterruptibly(tagFuture);

        Map<String, String> tags2 = ImmutableMap.of("t3", "3", "t4", "");
        tagFuture = metricsService.tagAvailabilityData(metric, tags2, start.plusMinutes(3).getMillis(),
            start.plusMinutes(5).getMillis());
        getUninterruptibly(tagFuture);

        ListenableFuture<AvailabilityMetric> queryFuture = metricsService.findAvailabilityData(metric,
                start.getMillis(), end.getMillis());
        AvailabilityMetric actualMetric = getUninterruptibly(queryFuture);
        List<Availability> actual = actualMetric.getData();
        List<Availability> expected = asList(
            new Availability(start.plusMinutes(6).getMillis(), UP),
            new Availability(start.plusMinutes(5).getMillis(), UP),
            new Availability(start.plusMinutes(4).getMillis(), DOWN),
            new Availability(start.plusMinutes(3).getMillis(), UP),
            new Availability(start.plusMinutes(2).getMillis(), DOWN),
            new Availability(start.plusMinutes(1).getMillis(), DOWN),
            new Availability(start.getMillis(), UP)
        );

        assertEquals(actual, expected, "The data does not match the expected values");
        assertEquals(actual.get(3).getTags(), tags2, "The tags do not match");
        assertEquals(actual.get(2).getTags(), tags2, "The tags do not match");
        assertEquals(actual.get(2).getTags(), tags2, "The tags do not match");
        assertEquals(actual.get(4).getTags(), tags1, "The tags do not match");
    }

    @Test
    public void tagNumericDataByDateRangeAndQueryByMultipleTags() throws Exception {
        String tenant = "tag-test";
        DateTime start = now().minusMinutes(20);

        getUninterruptibly(metricsService.createTenant(new Tenant().setId(tenant)));

        NumericData d1 = new NumericData(start.getMillis(), 101.1);
        NumericData d2 = new NumericData(start.plusMinutes(2).getMillis(), 101.2);
        NumericData d3 = new NumericData(start.plusMinutes(6).getMillis(), 102.2);
        NumericData d4 = new NumericData(start.plusMinutes(8).getMillis(), 102.3);
        NumericData d5 = new NumericData(start.plusMinutes(4).getMillis(), 102.1);
        NumericData d6 = new NumericData(start.plusMinutes(4).getMillis(), 101.4);
        NumericData d7 = new NumericData(start.plusMinutes(10).getMillis(), 102.4);
        NumericData d8 = new NumericData(start.plusMinutes(6).getMillis(), 103.1);
        NumericData d9 = new NumericData(start.plusMinutes(7).getMillis(), 103.1);

        NumericMetric m1 = new NumericMetric(tenant, new MetricId("m1"));
        m1.addData(d1);
        m1.addData(d2);
        m1.addData(d6);

        NumericMetric m2 = new NumericMetric(tenant, new MetricId("m2"));
        m2.addData(d3);
        m2.addData(d4);
        m2.addData(d5);
        m2.addData(d7);

        NumericMetric m3 = new NumericMetric(tenant, new MetricId("m3"));
        m3.addData(d8);
        m3.addData(d9);

        ListenableFuture<Void> insertFuture = metricsService.addNumericData(asList(m1, m2, m3));
        getUninterruptibly(insertFuture);

        Map<String, String> tags1 = ImmutableMap.of("t1", "1");
        Map<String, String> tags2 = ImmutableMap.of("t2", "2");

        ListenableFuture<List<NumericData>> tagFuture1 = metricsService.tagNumericData(m1, tags1, start.getMillis(),
            start.plusMinutes(6).getMillis());
        ListenableFuture<List<NumericData>> tagFuture2 = metricsService.tagNumericData(m2, tags1, start.getMillis(),
            start.plusMinutes(6).getMillis());
        ListenableFuture<List<NumericData>> tagFuture3 = metricsService.tagNumericData(m1, tags2,
            start.plusMinutes(4).getMillis(), start.plusMinutes(8).getMillis());
        ListenableFuture<List<NumericData>> tagFuture4 = metricsService.tagNumericData(m2, tags2,
            start.plusMinutes(4).getMillis(), start.plusMinutes(8).getMillis());
        ListenableFuture<List<NumericData>> tagFuture5 = metricsService.tagNumericData(m3, tags2,
            start.plusMinutes(4).getMillis(), start.plusMinutes(8).getMillis());

        getUninterruptibly(tagFuture1);
        getUninterruptibly(tagFuture2);
        getUninterruptibly(tagFuture3);
        getUninterruptibly(tagFuture4);
        getUninterruptibly(tagFuture5);

        ListenableFuture<Map<MetricId, Set<NumericData>>> queryFuture = metricsService.findNumericDataByTags(tenant,
                ImmutableMap.of("t1", "1", "t2", "2"));
        Map<MetricId, Set<NumericData>> actual = getUninterruptibly(queryFuture);
        ImmutableMap<MetricId, ImmutableSet<NumericData>> expected = ImmutableMap.of(
            new MetricId("m1"), ImmutableSet.of(d1, d2, d6),
            new MetricId("m2"), ImmutableSet.of(d5, d3)
        );

        assertEquals(actual, expected, "The tagged data does not match");
    }

    @Test
    public void tagAvailabilityByDateRangeAndQueryByMultipleTags() throws Exception {
        String tenant = "tag-test";
        DateTime start = now().minusMinutes(20);

        getUninterruptibly(metricsService.createTenant(new Tenant().setId(tenant)));

        AvailabilityMetric m1 = new AvailabilityMetric(tenant, new MetricId("m1"));
        AvailabilityMetric m2 = new AvailabilityMetric(tenant, new MetricId("m2"));
        AvailabilityMetric m3 = new AvailabilityMetric(tenant, new MetricId("m3"));

        Availability a1 = new Availability(start.getMillis(), UP);
        Availability a2 = new Availability(start.plusMinutes(2).getMillis(), UP);
        Availability a3 = new Availability(start.plusMinutes(6).getMillis(), DOWN);
        Availability a4 = new Availability(start.plusMinutes(8).getMillis(), DOWN);
        Availability a5 = new Availability(start.plusMinutes(4).getMillis(), UP);
        Availability a6 = new Availability(start.plusMinutes(4).getMillis(), DOWN);
        Availability a7 = new Availability(start.plusMinutes(10).getMillis(), UP);
        Availability a8 = new Availability(start.plusMinutes(6).getMillis(), DOWN);
        Availability a9 = new Availability(start.plusMinutes(7).getMillis(), UP);

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

        ListenableFuture<List<Availability>> tagFuture1 = metricsService.tagAvailabilityData(m1, tags1,
            start.getMillis(), start.plusMinutes(6).getMillis());
        ListenableFuture<List<Availability>> tagFuture2 = metricsService.tagAvailabilityData(m2, tags1,
            start.getMillis(), start.plusMinutes(6).getMillis());
        ListenableFuture<List<Availability>> tagFuture3 = metricsService.tagAvailabilityData(m1, tags2,
            start.plusMinutes(4).getMillis(), start.plusMinutes(8).getMillis());
        ListenableFuture<List<Availability>> tagFuture4 = metricsService.tagAvailabilityData(m2, tags2,
            start.plusMinutes(4).getMillis(), start.plusMinutes(8).getMillis());
        ListenableFuture<List<Availability>> tagFuture5 = metricsService.tagAvailabilityData(m3, tags2,
            start.plusMinutes(4).getMillis(), start.plusMinutes(8).getMillis());

        getUninterruptibly(tagFuture1);
        getUninterruptibly(tagFuture2);
        getUninterruptibly(tagFuture3);
        getUninterruptibly(tagFuture4);
        getUninterruptibly(tagFuture5);

        ListenableFuture<Map<MetricId, Set<Availability>>> queryFuture = metricsService.findAvailabilityByTags(tenant,
                ImmutableMap.of("t1", "1", "t2", "2"));
        Map<MetricId, Set<Availability>> actual = getUninterruptibly(queryFuture);
        ImmutableMap<MetricId, ImmutableSet<Availability>> expected = ImmutableMap.of(
            new MetricId("m1"), ImmutableSet.of(a1, a2, a6),
            new MetricId("m2"), ImmutableSet.of(a5, a3)
        );

        assertEquals(actual, expected, "The tagged data does not match");
    }

    @Test
    public void tagIndividualNumericDataPoints() throws Exception {
        String tenant = "tag-test";
        DateTime start = now().minusMinutes(20);

        getUninterruptibly(metricsService.createTenant(new Tenant().setId(tenant)));

        NumericData d1 = new NumericData(start.getMillis(), 101.1);
        NumericData d2 = new NumericData(start.plusMinutes(2).getMillis(), 101.2);
        NumericData d3 = new NumericData(start.plusMinutes(6).getMillis(), 102.2);
        NumericData d4 = new NumericData(start.plusMinutes(8).getMillis(), 102.3);
        NumericData d5 = new NumericData(start.plusMinutes(4).getMillis(), 102.1);
        NumericData d6 = new NumericData(start.plusMinutes(4).getMillis(), 101.4);
        NumericData d7 = new NumericData(start.plusMinutes(10).getMillis(), 102.4);
        NumericData d8 = new NumericData(start.plusMinutes(6).getMillis(), 103.1);
        NumericData d9 = new NumericData(start.plusMinutes(7).getMillis(), 103.1);

        NumericMetric m1 = new NumericMetric(tenant, new MetricId("m1"));
        m1.addData(d1);
        m1.addData(d2);
        m1.addData(d6);

        NumericMetric m2 = new NumericMetric(tenant, new MetricId("m2"));
        m2.addData(d3);
        m2.addData(d4);
        m2.addData(d5);
        m2.addData(d7);

        NumericMetric m3 = new NumericMetric(tenant, new MetricId("m3"));
        m3.addData(d8);
        m3.addData(d9);


        ListenableFuture<Void> insertFuture = metricsService.addNumericData(asList(m1, m2, m3));
        getUninterruptibly(insertFuture);

        Map<String, String> tags1 = ImmutableMap.of("t1", "");
        ListenableFuture<List<NumericData>> tagFuture = metricsService.tagNumericData(m1, tags1, d1.getTimestamp());
        assertEquals(getUninterruptibly(tagFuture), asList(d1), "Tagging " + d1 + " returned unexpected results");

        Map<String, String> tags2 = ImmutableMap.of("t1", "", "t2", "", "t3", "");
        tagFuture = metricsService.tagNumericData(m1, tags2, d2.getTimestamp());
        assertEquals(getUninterruptibly(tagFuture), asList(d2), "Tagging " + d2 + " returned unexpected results");

        tagFuture = metricsService.tagNumericData(m1, tags1, start.minusMinutes(10).getMillis());
        assertEquals(getUninterruptibly(tagFuture), Collections.emptyList(),
            "No data should be returned since there is no data for this time");

        Map<String, String> tags3 = ImmutableMap.of("t1", "", "t2", "");
        tagFuture = metricsService.tagNumericData(m2, tags3, d3.getTimestamp());
        assertEquals(getUninterruptibly(tagFuture), asList(d3), "Tagging " + d3 + " returned unexpected results");

        Map<String, String> tags4 = ImmutableMap.of("t3", "", "t4", "");
        tagFuture = metricsService.tagNumericData(m2, tags4, d4.getTimestamp());
        assertEquals(getUninterruptibly(tagFuture), asList(d4), "Tagging " + d4 + " returned unexpected results");

        ListenableFuture<Map<MetricId, Set<NumericData>>> queryFuture = metricsService.findNumericDataByTags(tenant,
                ImmutableMap.of("t2", "", "t3", ""));
        Map<MetricId, Set<NumericData>> actual = getUninterruptibly(queryFuture);
        ImmutableMap<MetricId, ImmutableSet<NumericData>> expected = ImmutableMap.of(
            new MetricId("m1"), ImmutableSet.of(d2),
            new MetricId("m2"), ImmutableSet.of(d3, d4)
        );

        assertEquals(actual, expected, "The tagged data does not match");
    }

    @Test
    public void tagIndividualAvailabilityDataPoints() throws Exception {
        String tenant = "tag-test";
        DateTime start = now().minusMinutes(20);

        getUninterruptibly(metricsService.createTenant(new Tenant().setId(tenant)));

        AvailabilityMetric m1 = new AvailabilityMetric(tenant, new MetricId("m1"));
        AvailabilityMetric m2 = new AvailabilityMetric(tenant, new MetricId("m2"));
        AvailabilityMetric m3 = new AvailabilityMetric(tenant, new MetricId("m3"));

        Availability a1 = new Availability(start.getMillis(), UP);
        Availability a2 = new Availability(start.plusMinutes(2).getMillis(), UP);
        Availability a3 = new Availability(start.plusMinutes(6).getMillis(), DOWN);
        Availability a4 = new Availability(start.plusMinutes(8).getMillis(), DOWN);
        Availability a5 = new Availability(start.plusMinutes(4).getMillis(), UP);
        Availability a6 = new Availability(start.plusMinutes(4).getMillis(), DOWN);
        Availability a7 = new Availability(start.plusMinutes(10).getMillis(), UP);
        Availability a8 = new Availability(start.plusMinutes(6).getMillis(), DOWN);
        Availability a9 = new Availability(start.plusMinutes(7).getMillis(), UP);

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
        ListenableFuture<List<Availability>> tagFuture = metricsService.tagAvailabilityData(m1, tags1,
            a1.getTimestamp());
        assertEquals(getUninterruptibly(tagFuture), asList(a1), "Tagging " + a1 + " returned unexpected results");

        Map<String, String> tags2 = ImmutableMap.of("t1", "", "t2", "", "t3", "");
        tagFuture = metricsService.tagAvailabilityData(m1, tags2, a2.getTimestamp());
        assertEquals(getUninterruptibly(tagFuture), asList(a2), "Tagging " + a2 + " returned unexpected results");

        tagFuture = metricsService.tagAvailabilityData(m1, tags1, start.minusMinutes(10).getMillis());
        assertEquals(getUninterruptibly(tagFuture), Collections.emptyList(),
            "No data should be returned since there is no data for this time");

        Map<String, String> tags3 = ImmutableMap.of("t2", "", "t3", "");
        tagFuture = metricsService.tagAvailabilityData(m2, tags3, a3.getTimestamp());
        assertEquals(getUninterruptibly(tagFuture), asList(a3), "Tagging " + a3 + " returned unexpected results");

        Map<String, String> tags4 = ImmutableMap.of("t3", "", "t4", "");
        tagFuture = metricsService.tagAvailabilityData(m2, tags4, a4.getTimestamp());
        assertEquals(getUninterruptibly(tagFuture), asList(a4), "Tagging " + a4 + " returned unexpected results");

        ListenableFuture<Map<MetricId, Set<Availability>>> queryFuture = metricsService.findAvailabilityByTags(tenant,
            tags3);
        Map<MetricId, Set<Availability>> actual = getUninterruptibly(queryFuture);
        ImmutableMap<MetricId, ImmutableSet<Availability>> expected = ImmutableMap.of(
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

        NumericMetric m1 = new NumericMetric(tenantId, new MetricId("m1"));
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

        getUninterruptibly(metricsService.addNumericData(asList(m1)));

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

    private void assertMetricEquals(Metric actual, Metric expected) {
        assertEquals(actual, expected, "The metric doe not match the expected value");
        assertEquals(actual.getData(), expected.getData(), "The data does not match the expected values");
    }

    private void assertMetricIndexMatches(String tenantId, MetricType type, List<? extends Metric> expected)
        throws Exception {
        ListenableFuture<List<Metric<?>>> metricsFuture = metricsService.findMetrics(tenantId, type);
        List<Metric<?>> actualIndex = getUninterruptibly(metricsFuture);

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
        ResultSetFuture queryFuture = dataAccess.findMetricsByTag(tenantId, tag);
        ResultSet resultSet = getUninterruptibly(queryFuture);
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

        private int numericTTL;

        private int numericTagTTL;

        private int availabilityTTL;

        private int availabilityTagTTL;

        public VerifyTTLDataAccess(DataAccess instance) {
            super(instance);
            numericTTL = DEFAULT_TTL;
            numericTagTTL = DEFAULT_TTL;
            availabilityTTL = DEFAULT_TTL;
            availabilityTagTTL = DEFAULT_TTL;
        }

        public void setNumericTTL(int expectedTTL) {
            this.numericTTL = expectedTTL;
        }

        public void numericTagTTLLessThanEqualTo(int numericTagTTL) {
            this.numericTagTTL = numericTagTTL;
        }

        public void setAvailabilityTTL(int availabilityTTL) {
            this.availabilityTTL = availabilityTTL;
        }

        public void availabilityTagTLLLessThanEqualTo(int availabilityTagTTL) {
            this.availabilityTagTTL = availabilityTagTTL;
        }

        @Override
        public ResultSetFuture insertData(NumericMetric metric, int ttl) {
            assertEquals(ttl, numericTTL, "The numeric data TTL does not match the expected value when " +
                "inserting data");
            return super.insertData(metric, ttl);
        }

        @Override
        public ResultSetFuture insertData(AvailabilityMetric metric, int ttl) {
            assertEquals(ttl, availabilityTTL, "The availability data TTL does not match the expected value when " +
                "inserting data");
            return super.insertData(metric, ttl);
        }

        @Override
        public ResultSetFuture insertNumericTag(String tag, String tagValue, NumericMetric metric,
                List<NumericData> data) {
            for (NumericData d : data) {
                assertTrue(d.getTTL() <= numericTagTTL, "Expected the TTL to be <= " + numericTagTTL +
                    " but it was " + d.getTTL());
            }
            return super.insertNumericTag(tag, tagValue, metric, data);
        }

        @Override
        public ResultSetFuture insertAvailabilityTag(String tag, String tagValue, AvailabilityMetric metric,
                List<Availability> data) {
            for (Availability a : data) {
                assertTrue(a.getTTL() <= availabilityTagTTL, "Expected the TTL to be <= " + availabilityTagTTL +
                    " but it was " + a.getTTL());
            }
            return super.insertAvailabilityTag(tag, tagValue, metric, data);
        }
    }

}
